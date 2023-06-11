import asyncio
import datetime
import json
import os
import re
from traceback import format_exc
import uuid

from faust import App, Record
from faust.types.web import ResourceOptions
from faust.web import View
from kafka import KafkaProducer

import analyze
from conf import settings
from utils.classes.FileWriter import FileWriter
from utils.classes.WorkTableAccessor import WorkTableAccessor
from .modules import get as get_module
from .modules import hash as hash_module
from .modules import helper as helper_module
from .modules import pipeline as pipeline_module
from .modules import put as put_module
from .modules import split as split_module
from .modules import t1 as t1_module
from .modules import noop as noop_module
from .modules import zip as zip_module


class ValueModel(Record):
    id: str
    status: str
    request_data: str
    response_data: str
    operation: str

kafka_max_rec_size = 50000

app = App(settings.REDACT_KAFKA_TOPIC_NAME,
    broker='kafka://'+settings.REDACT_KAFKA_SERVER_PATH,
    store='rocksdb://',
    web_cors_options={
        '*': ResourceOptions(
            expose_headers="*",
            allow_headers="*",
        )
    },
)

rf_topic = app.topic(
    settings.REDACT_KAFKA_TOPIC_NAME,
    key_type=bytes,
    value_type=ValueModel,
)

work_table = app.Table('redact-work', default=str)

fw = FileWriter()
wta = WorkTableAccessor(work_table)

@app.agent(rf_topic)
async def dispatch_agent(stream):
    async for key, value in stream.group_by(ValueModel.id).items():
        if value.status == 'init':
            print('---------kafka agent processing init for {}:{}'.format(key, value))
            await process_start(key.decode('utf-8'), value)
        elif value.status == 'clear-table':
            print('--------- kafka agent: processing clear table: {}'.format(value))
            await clear_table_entries(value)
        elif value.status == 'kill':
            print('---------kafka agent processing kill for {}'.format(key))
            await process_kill(key.decode('utf-8'))
        else:
            print('UNKNOWN STATUS: {}---{}'.format(key.decode('utf-8'), value))

############################################## TOP LEVEL CALLPOINTS
async def get_killed_jobs(): #TODO move to helper_module
    sig_rec = await wta.work_table_read_async('signals')
    if sig_rec:
        return sig_rec.get('killed_jobs', [])
    else:
        return []

async def process_start(job_key, data_in={}):
    print(f'processing start for {job_key}')
    input = {}
    try:
        input = await pipeline_module.get_pipeline_input_data(job_key, fw, wta, data_in)
        if input.get('movies'):
            input['movies'] = await helper_module.make_local_copies_of_input(input)
        pipeline = await pipeline_module.get_or_create_pipeline(input, job_key, fw, wta)
        if not pipeline:
            # todo delete the new files listed in new_movies IF they're not the same as what the caller supplied
            return await close_pipeline_with_error(job_key, 'unable to build')
        nodes_by_depth, cur_tier = helper_module.get_nodes_by_depth(pipeline)
        tier_keys_to_process = sorted(nodes_by_depth.keys())
        for tier_number in tier_keys_to_process:
            killed_jobs = await get_killed_jobs()
            print('killed jobs are {}'.format(killed_jobs))
            if not await wta.pipeline_is_alive(job_key):
                return await close_pipeline_with_error(job_key, 'quit requested')
            jobs_to_run = []
            node_row = nodes_by_depth[tier_number]
            for node_id in node_row:
                jobs_to_run += await build_jobs_for_node(job_key, node_id, pipeline, input)
            if not jobs_to_run:
                err_str = f'ERROR: no jobs to run for tier {tier_number}'
                return await close_pipeline_with_error(job_key, err_str)
            print('running jobs for depth {}'.format(tier_number))
            responses = await asyncio.gather(*jobs_to_run)
            await compile_responses(node_row, pipeline, job_key)
        await wrap_up_pipeline(job_key, pipeline, node_row, data_in)
    except Exception as e:
        print('^^Crash in process_start for job {}: {} '.format(job_key, format_exc()))
    finally:
        if not input.get('job_lifecycle_data', {}).get('preserve_intermediate_output'):
            await helper_module.clear_this_jobs_table_records(job_key, wta)

async def compile_responses(node_row, pipeline, job_key):
    nodes_to_compile = []
    for node_id in node_row:
        job_type = pipeline['node_metadata']['node'][node_id]['type']
        if job_type == 'hash':
            await hash_module.compile_and_save_responses(job_key, node_id, wta)
            continue
        elif job_type in ['fetch', 'put', 'zip']:
            await helper_module.compile_and_save_whole_movie_responses(job_key, node_id, fw, wta)
            continue
        node_wt_key = job_key + '-' + node_id
        node_rec = await wta.work_table_read_async(node_wt_key)
        if 'job_ids' not in node_rec:
            # they're subprocess jobs. get their payloads later, from their responses 
            if node_wt_key in work_table.keys():
                node_rec['status'] = 'complete'
                await wta.work_table_write_async(node_wt_key, node_rec)
            continue
        build_response = {}
        job_ids = node_rec.get('job_ids')
        for child_job_id in job_ids:
            child_rec = await wta.work_table_read_async(child_job_id)
            if child_rec['status'] == 'complete':
                child_data = child_rec['data']
                if not build_response:
                    build_response = child_data
                else:
                    if 'movies' in child_data:
                        helper_module.merge_child_movie_data(child_data, build_response)
        await helper_module.save_node_response_data(job_key, node_id, build_response, fw, wta)

async def clear_table_entries(value):
    try:
        print(f'clearing table entries {value}')
        for key in list(work_table.keys()):
            print(f'one key is {key}')
            if key not in work_table:
                continue # it's been deleted earlier in an earlier iteration
            key_rec = await wta.work_table_read_async(key)
            delete_date_string = key_rec.get('delete_by')
            if delete_date_string:
                delete_date = datetime.datetime.strptime(delete_date_string, "%Y-%m-%d %H:%M:%S")
                if delete_date < datetime.datetime.now():
                    print(f'deleting table entry for aged out job: {key}')
                    await helper_module.clear_this_jobs_table_records(key, wta)
                    del work_table[key]
        wt_keys = []
        if value.request_data and len(value.request_data) > 2:
            try:
                rd = json.loads(value.request_data)
                wt_keys = rd.get('keys', [])
            except: 
                pass
        print('wt keys are {}'.format(wt_keys))
        for key in list(work_table.keys()):
            print(f'considering {key}')
            if 'all' in wt_keys or key in wt_keys:
                print(f'clearing {key}')
                work_table.pop(key)
        print('done clearing table entries')
    except Exception as e:
        print(format_exc())

async def process_kill(job_key):
    signals_rec = await wta.work_table_read_async('signals')
    if not signals_rec:
        signals_rec = {
            'killed_jobs': [],
        }
    print(f'killing job {job_key}')
    if job_key not in signals_rec['killed_jobs']:
        signals_rec['killed_jobs'].append(job_key)
    await wta.work_table_write_async('signals', signals_rec)
    print('killed jobs are now {}'.format(signals_rec['killed_jobs']))

############################################## MISC UTILS
def print_table(message, show_table_rows=False):
    data = {
        'table_keys': sorted(work_table.keys()),
        'table_meta': app.router.table_metadata('redact-work'),
    }
    print('========== {} current table: {} '.format(message, data))
    if not show_table_rows:
        return
    if work_table.keys():
        for x in work_table.items():
            print('  table record: {}'.format(x))

############################################## PIPELINE MANAGEMENT TOOLS
async def build_child_pipeline_jobs(job_key, node_id, pipeline, parent_pipeline_input):
    jobs = []
    new_job_id = str(uuid.uuid4())
    node_rec = {
        'node_id': node_id,
        'type': 'node_data',
        'status': 'init',
        'job_ids': [new_job_id],
    }
    node_rec_work_table_key = job_key + '-' + node_id
    await wta.work_table_write_async(node_rec_work_table_key, node_rec)

    node_data = pipeline['node_metadata']['node'].get(node_id)
    new_pipeline_id = node_data.get('entity_id') # what the new pipeline is called wrt other pipelines
    new_pipeline = await pipeline_module.get_or_create_pipeline(
        parent_pipeline_input, new_job_id, fw, wta, node_rec_work_table_key, new_pipeline_id, node_id, job_key
    )
    if pipeline.get('debug'):
        print('building job for new sub pipeline: {}'.format(new_pipeline))
    jobs.append(process_start(new_job_id))

    return jobs

async def close_pipeline_with_error(job_key, error_string):
    # TODO look at cleaning up files now, or maybe on job delete instead
    print(f'closing pipeline with error ***{error_string}***')
    if job_key not in work_table:
        return
    pipe_rec = await wta.work_table_read_async(job_key)
    pipe_rec['response_data'] = {'errors': [error_string]}
    pipe_rec['status'] = 'failed'
    await wta.work_table_write_async(job_key, pipe_rec)

async def build_jobs_for_node(job_key, node_id, pipeline, parent_pipeline_input):
    jobs = []
    request_data = await wta.work_table_read_request_data_async(job_key)
    job_type = pipeline['node_metadata']['node'][node_id]['type']
    print('building jobs for node {} - {}'.format(node_id, job_type))
    if job_type == 'split': 
        jobs = await split_module.build_jobs(job_key, node_id, pipeline, work_table)
    elif job_type == 'fetch' or job_type == 'secure_files_import': 
        recording_ids = request_data.get('recording_ids')
        jobs = await get_module.build_jobs(job_key, node_id, request_data.get('recording_ids'), pipeline, work_table)
    elif job_type == 'put' or job_type == 'secure_files_export': 
        jobs = await put_module.build_jobs(job_key, node_id, pipeline, work_table)
    elif job_type == 'hash': 
        jobs = await hash_module.build_jobs(job_key, node_id, pipeline, work_table)
    elif job_type == 'noop': 
        jobs = await noop_module.build_jobs(job_key, node_id, request_data, pipeline, work_table)
    elif job_type == 'zip': 
        jobs = await zip_module.build_jobs(job_key, node_id, pipeline, work_table)
    elif job_type == 'pipeline': 
        jobs = await build_child_pipeline_jobs(job_key, node_id, pipeline, parent_pipeline_input)
    elif helper_module.is_t1_scanner(job_type):
        jobs = await t1_module.build_t1_scanner_jobs(job_key, job_type, node_id, pipeline, work_table)
    return jobs

async def wrap_up_pipeline(job_key, pipeline, final_node_row, valuemodel_in):
    if pipeline.get('debug'):
        print_table('JUST BEFORE WRAP UP PIPELINE', True)
    producer = KafkaProducer(bootstrap_servers=settings.REDACT_KAFKA_SERVER_PATH)
    response_data_url = ''
    print(f'wrapping up pipeline {job_key}')
    for node_id in final_node_row:
        node_wt_key = job_key + '-' + node_id
        node_rec = await wta.work_table_read_async(node_wt_key)
        if node_rec['status'] == 'complete':
            response_data = node_rec.get('data')
            if response_data.get('cached_file_fullpath'):
                file_fullpath = response_data['cached_file_fullpath']
            else:
                workdir_uuid = str(uuid.uuid4())
                file_name = 'response_data.json'
                file_fullpath = await fw.write_json_to_filesystem_async(workdir_uuid, file_name, response_data)
            response_data_url = fw.get_url_for_file_path(file_fullpath)
            # make sure all the paths in the response data are urls, not file paths
            response_data = await fw.read_json_async(file_fullpath)
            write_new_file = False
            for movie_url, movie in response_data.get('movies', {}).items():
                work_was_done = helper_module.add_urls_to_movie(movie, fw)
                if work_was_done:
                    write_new_file = True
            if write_new_file:
                workdir_uuid = fw.get_uuid_directory_from_url(response_data_url)
                await fw.write_json_to_filesystem_async(workdir_uuid, 'response_data.json', response_data)
    pipeline_wt_obj = await wta.work_table_read_async(job_key)
    pipeline_wt_obj['status'] = 'complete'
    if pipeline_wt_obj.get('created'):
        start_time = datetime.datetime.strptime(pipeline_wt_obj['created'], "%Y-%m-%d %H:%M:%S")
        run_time_seconds = round((datetime.datetime.now() - start_time).total_seconds(), 2)
        pipeline_wt_obj['run_time_seconds'] = run_time_seconds
    request_data = ''
    if type(valuemodel_in) == dict: # it's a child pipeline
        request_data = valuemodel_in.get('request_data')
    else: # its a top level pipeline
        if getattr(valuemodel_in, 'request_data'):
            request_data = valuemodel_in.request_data
    pipeline_wt_obj['data'] = {
        'request_data_url': request_data,
        'response_data_url': response_data_url,
    }
    print('response data url is {}'.format(response_data_url))
    await wta.work_table_write_async(job_key, pipeline_wt_obj)

    pipeline_req_data = await pipeline_module.get_pipeline_input_data(job_key, fw, wta)
    if pipeline_req_data and pipeline_req_data.get('callback_url'):
        await do_callback(job_key, pipeline_req_data.get('callback_url'), response_data_url)

async def do_callback(job_key, callback_url, response_data_url):
    headers = {
        'Authorization': 'Api-Key ' + settings.GUIDED_REDACTION_API_KEY,
    }
    print(f'doing callback to {callback_url}')
    build_obj = {
        'job_id': job_key,
        'response_data_url': response_data_url,
    }
    await fw.post_data_to_url_async(callback_url, build_obj, headers=headers)
    
############################################## WEB ENDPOINTS
@app.page('/api/redact/v1/dispatch')
class DispatchView(View):
    async def post(self, request):
        try:
            producer = KafkaProducer(bootstrap_servers=settings.REDACT_KAFKA_SERVER_PATH)
            workdir_uuid = str(uuid.uuid4())
            workdir = fw.create_unique_directory(workdir_uuid)
            json_filename = 'request_data.json'
            json_file_fullpath = fw.build_file_fullpath_for_uuid_and_filename(workdir_uuid, json_filename)
            request_data = await request.json()
            operation = request_data.get('operation', 'unknown')
            await fw.write_json_to_filesystem_async(workdir_uuid, json_filename, request_data)
            download_url = fw.get_url_for_file_path(json_file_fullpath)

            kafka_obj = {
               'id': workdir_uuid,
               'status': 'init',
               'request_data': download_url,
               'response_data': '',
               'operation': operation,
            }
            job_key = str.encode(workdir_uuid)
            send_kafka_bytes = json.dumps(kafka_obj).encode('utf-8')
            future = producer.send(settings.REDACT_KAFKA_TOPIC_NAME, key=job_key, value=send_kafka_bytes)

            future.get(timeout=10)
            return self.json({'job_id': workdir_uuid})
        except Exception as e:
            error_uuid = str(uuid.uuid4())
            print('error number {} failed initiating request: {}'.format(error_uuid, e))
            error_str = 'an error was encountered.  tracking id: {}'.format(error_uuid)
            return self.json({'errors': [error_str]})

@app.page('/api/redact/v1/cancel_faust_job/{job_key}')
class KillJobView(View):
    async def get(self, request, job_key):
        try:
            producer = KafkaProducer(bootstrap_servers=settings.REDACT_KAFKA_SERVER_PATH)
            kafka_obj = {
               'id': job_key,
               'status': 'kill',
               'request_data': '',
               'response_data': '',
               'operation': '',
            }
            send_kafka_bytes = json.dumps(kafka_obj).encode('utf-8')
            future = producer.send(settings.REDACT_KAFKA_TOPIC_NAME, key=job_key.encode('utf-8'), value=send_kafka_bytes)
            future.get(timeout=10)
            return self.json({'result': 'kill request submitted'})
        except Exception as e:
            error_uuid = str(uuid.uuid4())
            print('error number {} failed initiating request: {}'.format(error_uuid, e))
            error_str = 'an error was encountered.  tracking id: {}'.format(error_uuid)
            return self.json({'errors': [error_str]})

@app.page('/api/redact/v1/clear-table')
class ClearTableView(View):
    async def post(self, request):
        producer = KafkaProducer(
            bootstrap_servers=settings.REDACT_KAFKA_SERVER_PATH,
        )
        req_data = await request.json()
        if len(json.dumps(req_data)) > kafka_max_rec_size:
            er_str = 'kafka topic size limit of {kafka_max_rec_size} prevented the request being submitted, reduce payload size'
            return Response({'errors': [er_str]})
        workdir_uuid = str(uuid.uuid4())
        kafka_obj = {
           'id': workdir_uuid,
           'status': 'clear-table',
           'request_data': json.dumps(req_data),
           'response_data': '',
           'operation': 'clear-table',
        }
        job_key = str.encode(workdir_uuid)
        send_kafka_bytes = json.dumps(kafka_obj).encode('utf-8')
        future = producer.send(settings.REDACT_KAFKA_TOPIC_NAME, key=job_key, value=send_kafka_bytes)
        try:
            future.get(timeout=10)
            return self.json({'job_id': workdir_uuid})
        except Exception as e:
            print('kafka topic write failed: {}'.format(e))
            return self.json({'errors': [e]})

@app.page('/api/redact/v1/faust_jobs/{job_id}')
class JobView(View):
    async def get(self, request, job_id):
        build_obj = {}
        table_rec = await wta.work_table_read_async(job_id)
        if table_rec:
            build_obj = build_rec_from_work_table_item(table_rec, job_id, '', True)
        return self.json(build_obj)

@app.page('/api/redact/v1/faust_jobs')
class JobsView(View):
    async def get(self, request):
        build_obj = {}
        for table_key, table_rec in work_table.items():
            if table_rec['type'] != 'pipeline':
                continue
            info_rec = build_rec_from_work_table_item(table_rec, table_key, request.url, False)
            build_obj[table_key] = info_rec
        for table_key, table_rec in work_table.items():
            if table_rec['type'] != 'node_data':
                continue
            info_rec = build_rec_from_work_table_item(table_rec, table_key, request.url, False)
            build_obj[table_key] = info_rec
        for table_key, table_rec in work_table.items():
            if table_rec['type'] in ['node_data', 'pipeline']:
                continue
            info_rec = build_rec_from_work_table_item(table_rec, table_key, request.url, False)
            build_obj[table_key] = info_rec
        return self.json(build_obj)

def build_rec_from_work_table_item(table_rec, table_key, base_url, include_data):
    info_rec = {}
    info_rec = table_rec
    if not include_data:
        info_rec['data'] = '<withheld>'
        if info_rec.get('request_data'):
            info_rec['request_data'] = '<withheld>'
        info_rec['view_detail_link'] =  str(base_url) + '/' + table_key
    return info_rec

############################################## 
