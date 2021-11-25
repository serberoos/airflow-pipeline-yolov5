from datetime import datetime, timedelta 
from kubernetes.client import models as k8s
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod import Resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

task_default_args= {
    'owner': 'airflow',
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 11, 25), 
    'depends_on_past': False, 
    'email' : ['airflow@example.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    'execution_timeout': timedelta(hours=1),
    'provide_context':True, 
}

yolo_task_list = "{{ dag_run.conf.yolo_task_list }}"
yolo_task_list = eval(yolo_task_list)
yolo_kubepod = []

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

dag = DAG(
    dag_id='dynamic-yolov5-pipeline', 
    description='ML pipeline of YOLOv5', 
    default_args=task_default_args,
    render_template_as_native_obj=True, 
    schedule_interval='5 16 * * *', 

    max_active_runs=1
)

pod_resources = Resources() 
pod_resources.request_cpu = '10000m'
pod_resources.request_memory = '20480Mi'
pod_resources.limit_cpu = '20000m'
pod_resources.limit_memory = '40960Mi'

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
yolo_pipeline_start = DummyOperator(task_id="yolo_pipeline_start", dag=dag)

yolo_pipeline_end = DummyOperator(task_id="yolo_pipeline_end", dag=dag)


for yolo_task, yolo_task_num in zip(yolo_task_list ,range(0, len(yolo_task_list))):

    train_kubepod = KubernetesPodOperator(
        task_id=f"train_kubepod_{yolo_task_num}",
        name=f"train_kubepod_{yolo_task_num}",
        namespace='airflow', 
        image='jae99c/yolov5-pipeline', 
        cmds=["/bin/sh", "-c"], 
        arguments=[f"python train.py --img { yolo_task['train']['img'] } \
        --batch { yolo_task['train']['batch'] } \
        --epochs { yolo_task['train']['epochs'] } \
        --data /usr/src/app/yolo_pipeline_volume/dataset/data.yaml \
        --cfg ./models/yolov5s.yaml \
        --weights yolov5s.pt \
        --name yolo_result_{yolo_task_num}; \
        cp -r /usr/src/app/runs/train/yolo_result_{yolo_task_num} /usr/src/app/yolo_pipeline_volume/train_result/yolo_result_{yolo_task_num}"], 
        
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

    detect_kubepod = KubernetesPodOperator(
        task_id=f"detect_kubepod_{yolo_task_num}",
        name=f"detect_kubepod_{yolo_task_num}",
        namespace='airflow',
        image='jae99c/yolov5-pipeline',
        cmds=["/bin/sh", "-c"],
        arguments=[f"python detect.py --source /usr/src/app/yolo_pipeline_volume/input/result_{ yolo_task_num }.mp4 \
            --weights /usr/src/app/yolo_pipeline_volume/train_result/mask_yolo_result/weights/best.pt \
            --img { yolo_task['detect']['img'] } \
            --conf { yolo_task['detect']['conf'] }; \
            cp /usr/src/app/runs/detect/exp/result_{ yolo_task_num }.mp4 /usr/src/app/yolo_pipeline_volume/detect_result/result_{ yolo_task_num }.mp4"], 
        labels={"foo": "bar"},
        in_cluster=True,
        is_delete_operator_pod=True,
        get_logs=True, 
        image_pull_policy='IfNotPresent',
        resources=pod_resources,
        startup_timeout_seconds=500, 
        volumes=[yolo_volume],
        volume_mounts=[yolo_volume_mount],
        dag=dag,
    )
    yolo_kubepod.append([train_kubepod,detect_kubepod])




yolo_pipeline_start >> yolo_kubepod >> yolo_pipeline_end >> upload_to_google_drive_kubepod



"""
    # dag_run.conf input json example
{
  "yolo_task_list": [            
    {
      "train": {
        "img": "416",
        "batch": "8",
        "epochs": "50"
      },
      "detect":{
        "img": "416",
        "conf": "0.5"
      }
    },

    {
      "train": {
        "img": "416",
        "batch": "8",
        "epochs": "50"
      },
      "detect":{
        "img": "416",
        "conf": "0.5"
      }
    },

    {
      "train": {
        "img": "416",
        "batch": "8",
        "epochs": "50"
      },
      "detect":{
        "img": "416",
        "conf": "0.5"
      }
    }
  ]
}
"""