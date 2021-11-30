from datetime import datetime, timedelta 
from kubernetes.client import models as k8s
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod import Resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

task_ids = ['task_0','task_1','task_2','task_3','task_4'] # pod task 수


# def json_to_yolo_arguments(json_tasks, task_ids, ti):

# 	for task_id in task_ids:
# 		train_img = json_tasks[f'{task_id}']['train']['img']
# 		train_batch = json_tasks[f'{task_id}']['train']['batch']
# 		train_epochs = json_tasks[f'{task_id}']['train']['epochs']
# 		train_arguments = (train_img, train_batch, train_epochs, task_id, task_id, task_id)
# 		ti.xcom_push(key=f"{task_id}_train_argument", value = train_arguments)
		
# 		detect_img = json_tasks[f'{task_id}']['detect']['img']
# 		detect_conf = json_tasks[f'{task_id}']['detect']['conf']
# 		detect_arguments = (task_id, detect_img, detect_conf, task_id)
# 		ti.xcom_push(key=f"{task_id}_detect_argument", value = detect_arguments)

# 	"""
# 	# # total_tasks = len(json_tasks.items())
# 	# global task_ids
# 	# task_ids = list(json_tasks.keys())
# 	# print(type(task_ids))
# 	"""

def get_yolo_train_arguments(task_id):
	train_img = f"dag_run.conf.yolo_tasks.{task_id}.train.img"
	train_batch = f"dag_run.conf.yolo_tasks.{task_id}.train.batch"
	train_epochs = f"dag_run.conf.yolo_tasks.{task_id}.train.epochs"

	return (train_img, train_batch, train_epochs, task_id, task_id, task_id)

def get_yolo_detect_arguments(task_id):
    input_filename = f"dag_run.conf.input_filename"
    detect_img = f"dag_run.conf.yolo_tasks.{task_id}.detect.img"
    detect_batch = f"dag_run.conf.yolo_tasks.{task_id}.detect.conf"

    return (input_filename, task_id, detect_img, detect_batch, input_filename, task_id)


task_default_args= {
    'owner': 'airflow',
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 11, 30), 
    'depends_on_past': False, 
    'email' : ['airflow@example.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    'execution_timeout': timedelta(hours=1),
    'provide_context':True, 
}

yolo_volume_mount = VolumeMount(name='yolo-volume', 
                            mount_path='/usr/src/app/yolo_pipeline_volume/',
                            sub_path=None,
                            read_only=False)
volume_config = {
    'persistentVolumeClaim':
        {
            'claimName': 'airflow-data-pvc' 
        }
}
yolo_volume = Volume(name='yolo-volume', configs=volume_config)

# affinity = {
#     'nodeAffinity': {
#       'preferredDuringSchedulingIgnoredDuringExecution': [  # 선호 affinity
#         {
#           "weight": 1, # 우선순위
#           "preference": {
#             "matchExpressions": { # 특정 worker node에 설정된 label과 해당 설정의 label이 매칭되는 worker node에 pod를 생성
#               "key": "airflow_yolo_pipeline", 
#               "operator": "In",
#               "values": ["airflow_yolo"] 
#               # gpu-bn880-1 : airflow_yolo_pipeline=airflow_yolo
#               # 해당 레이블이 있는 노드만 이용해 포드를 실행한다.
#             }
#           }
#         }
#       ]
#     }
# }

dag = DAG(
    dag_id='dynamic-yolov5-pipeline', 
    description='ML pipeline of YOLOv5', 
    default_args=task_default_args,
    render_template_as_native_obj=True, 
    schedule_interval='@daily', 

    max_active_runs=1
)

pod_resources = Resources() 
pod_resources.request_cpu = '10000m'
pod_resources.request_memory = '20480Mi'
pod_resources.limit_cpu = '20000m'
pod_resources.limit_memory = '40960Mi'

pipeline_start = DummyOperator(task_id="pipeline_start", dag=dag)

# get_yolo_arguments = PythonOperator(
#             task_id="get_yolo_arguments",
#             python_callable=json_to_yolo_arguments,
#             provide_context=True,
# 			op_args=["{{ dag_run.conf.yolo_tasks }}", task_ids],
#             dag=dag,
# )


train_kubepods = [
    KubernetesPodOperator(
        task_id=f"train_kubepod_{task_id}",
        name=f"train_kubepod_{task_id}",
        namespace='airflow', 
        image='jae99c/yolov5-pipeline', 
        cmds=["/bin/sh", "-c"], 

        arguments=["""
			 python train.py --img "{{ %s }}" \
            --batch "{{ %s }}" \
            --epochs "{{ %s }}" \
            --data /usr/src/app/yolo_pipeline_volume/dataset/data.yaml \
            --cfg ./models/yolov5s.yaml \
            --weights yolov5s.pt \
            --name train_result_%s; \
			cp -r /usr/src/app/runs/train/train_result_%s /usr/src/app/yolo_pipeline_volume/train_result/train_result_%s;
        """ % get_yolo_train_arguments(task_id)],

        labels={"foo": "bar"},
        in_cluster=True,
        
        image_pull_policy='IfNotPresent',
        is_delete_operator_pod=True, 
        get_logs=True, 
        resources=pod_resources, 
        startup_timeout_seconds=500, 
        volumes=[yolo_volume],
        volume_mounts=[yolo_volume_mount],
        # affinity=affinity,
        dag=dag,
    ) for task_id in task_ids ]

detect_kubepods = [
    KubernetesPodOperator(
        task_id=f"detect_kubepod_{task_id}",
        name=f"detect_kubepod_{task_id}",
        namespace='airflow',
        image='jae99c/yolov5-pipeline',
		# env_vars={
		# 	'YOLO_TASKS': '{{ dag_run.conf.yolo_tasks }}'
		# },
        cmds=["/bin/sh", "-c"], 
        arguments=["""
			python detect.py --source /usr/src/app/yolo_pipeline_volume/input/"{{ %s }}" \
            --weights /usr/src/app/yolo_pipeline_volume/train_result/train_result_%s/weights/best.pt \
            --img "{{ %s }}" \
            --conf "{{ %s }}"; \
            cp /usr/src/app/runs/detect/exp/mask.mp4 /usr/src/app/yolo_pipeline_volume/detect_result/{{ %s }}_detect_result_%s.mp4
			""" % get_yolo_detect_arguments(task_id)], 
	
            
        labels={"foo": "bar"},
        in_cluster=True,
        is_delete_operator_pod=True,
        get_logs=True, 
        image_pull_policy='IfNotPresent',
        resources=pod_resources,
        startup_timeout_seconds=500, 
        volumes=[yolo_volume],
        volume_mounts=[yolo_volume_mount],
        # affinity=affinity,
        dag=dag,
    ) for task_id in task_ids ]




upload_to_google_drive_kubepod = KubernetesPodOperator(
    task_id="upload_to_google_drive_kubepod", 
    name="upload_to_google_drive_kubepod",
    namespace='airflow',
    image='jae99c/yolov5-pipeline', 
    cmds=["/bin/sh", "-c"], 
    arguments=['python /usr/src/app/yolo_pipeline_volume/upload_to_google_drive.py' ], 
    labels={"foo": "bar"},
    in_cluster=True,
    image_pull_policy='IfNotPresent',
    is_delete_operator_pod=True, 
    get_logs=True, 
    resources=pod_resources,
    startup_timeout_seconds=500,
    volumes=[yolo_volume],
    volume_mounts=[yolo_volume_mount],
    dag=dag,
)

clear_volume_kubepod = KubernetesPodOperator(
    task_id="clear_volume_kubepod", 
    name="clear_volume_kubepod",
    namespace='airflow',
    image='jae99c/yolov5-pipeline', 
    cmds=["/bin/sh", "-c"], 
    arguments=["""
		rm -rf /usr/src/app/yolo_pipeline_volume/detect_result/*;
		rm -rf /usr/src/app/yolo_pipeline_volume/train_result/*
		"""], 

    labels={"foo": "bar"},
    in_cluster=True,
    image_pull_policy='IfNotPresent',
    is_delete_operator_pod=True, 
    get_logs=True, 
    resources=pod_resources,
    startup_timeout_seconds=500,
    volumes=[yolo_volume],
    volume_mounts=[yolo_volume_mount],
    dag=dag,
)

volume_kubepod = KubernetesPodOperator(
    task_id="volume_kubepod", 
    name="volume_kubepod",
    namespace='airflow',
    image='jae99c/yolov5-pipeline', 
    # cmds=["/bin/sh", "-c"], 
    # arguments=['sleep 100110' ], 
    labels={"foo": "bar"},
    in_cluster=True,
    image_pull_policy='IfNotPresent',
    is_delete_operator_pod=True, 
    get_logs=True, 
    resources=pod_resources,
    startup_timeout_seconds=500,
    volumes=[yolo_volume],
    volume_mounts=[yolo_volume_mount],
    dag=dag,
)

pipeline_end = DummyOperator(task_id="pipeline_end", dag=dag)

chain(pipeline_start, clear_volume_kubepod, train_kubepods, detect_kubepods, upload_to_google_drive_kubepod, pipeline_end)
pipeline_start >> volume_kubepod >> pipeline_end
"""
    # dag_run.conf input json example
    # v1
{
    "input_filename" : "mask.mp4",
	"yolo_tasks": {
		"task_0": {
			"train": {
				"img": "416",
				"batch": "2",
				"epochs": "10"
			},
			"detect": {
				"img": "416",
				"conf": "0.5"
			}
		},
		"task_1": {
			"train": {
				"img": "416",
				"batch": "2",
				"epochs": "20"
			},
			"detect": {
				"img": "416",
				"conf": "0.5"
			}
		},
		"task_2": {
			"train": {
				"img": "416",
				"batch": "2",
				"epochs": "30"
			},
			"detect": {
				"img": "416",
				"conf": "0.5"
			}
		},
		"task_3": {
			"train": {
				"img": "416",
				"batch": "2",
				"epochs": "40"
			},
			"detect": {
				"img": "416",
				"conf": "0.5"
			}
		},
		"task_4": {
			"train": {
				"img": "416",
				"batch": "2",
				"epochs": "50"
			},
			"detect": {
				"img": "416",
				"conf": "0.5"
			}
		}
	}
}
"""
# accuracies = ti.xcom_pull(key='model_accuracy', task_ids=['training_model_A', 'training_model_B', 'training_model_C'])