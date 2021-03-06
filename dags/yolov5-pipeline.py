from datetime import datetime, timedelta 
# datetime  : 스케줄링 된 시작날짜를 알기 위함.
# timedelta : 기준 시각에 전 시간 / 후 시간을 알기 위함.

from kubernetes.client import models as k8s
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod import Resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator # airflow.contrib.operatos.Kubernetes_pod_operator 는 구버전 | airflow 2.0 버전부터는 지원하지 않는다.
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

dag_id ='yolov5-pipeline'


# train_opt_img = "{{ dag_run.conf['train_img'] }}"
# train_opt_batch = "{{ dag_run.conf['train_batch'] }}"
# train_opt_epochs = "{{ dag_run.conf['train_epochs'] }}"
# detect_opt_img = "{{ dag_run.conf['detect_img'] }}"
# detect_opt_conf = "{{ dag_run.conf['detect_conf'] }}"


task_default_args= {
    'owner': 'airflow', # owner
    'retries': 3, # 재시도 횟수
    'retry_delay': timedelta(minutes=5), # 재 시작 딜레이
    'start_date': datetime(2019, 11, 20), # 시작 일
    'depends_on_past': False, # 이전 날짜의 task 인스턴스 중에서 동일한 task 인스턴스가 실패한 경우 실행되지 않고 대기.
    'email' : ['airflow@example.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    'execution_timeout': timedelta(hours=1),
    'provide_context':True, # XCom 사용    
}
# 컨테이너와 volume_mount
yolo_volume_mount = VolumeMount(name='yolo-volume', 
                            mount_path='/usr/src/app/yolo_pipeline_volume/',
                            sub_path=None,
                            read_only=False)
volume_config = {
    'persistentVolumeClaim':
        {
            'claimName': 'airflow-data-pvc' # PVC 명
        }
}
yolo_volume = Volume(name='yolo-volume', configs=volume_config) #volume 생성

dag = DAG(
    dag_id=dag_id, # 고유 식별자
    description='ML pipeline of YOLOv5', # 설명
    default_args=task_default_args,
    render_template_as_native_obj=True, # 템플릿이 파이썬 코드를
    schedule_interval='5 16 * * *', # DAG가 trigger될 빈도를 정의한다.

    # params={
    #     "train_img": "416",
    #     "train_img": "416",
    #     "train_batch": "2",
    #     "train_epochs": "1",
    #     "detect_img": "416",
    #     "detect_conf": "0.5"
    # },

    max_active_runs=1
)

# env = Secret(
#     'env',
#     'TEST',
#     'test-env',
#     'TEST',
# )

pod_resources = Resources() # 포드 가용 리소스 정의
pod_resources.request_cpu = '10000m'
pod_resources.request_memory = '20480Mi'
pod_resources.limit_cpu = '20000m'
pod_resources.limit_memory = '40960Mi'

"""
    pod의 가용가능 리소스가 한정되어 있다면 resource의 request limit을 정의할 수 있다.
    위처럼 resource로 설정할 수도 있고 kubernetes executor를 사용하고 있는 환경이라면 pod argument에 executor의 리소스를 정의할 수 있다.

    executor_config={
        "KubernetesExecutor": {
            "limit_cpu": "8",
            "limit_memory": "64Gi",
            "request_cpu": "4",
            "request_memory": "32Gi",
        }
    }

"""
# configmaps = [
#     k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='airflow-airflow-config')), #configmaps 가져오기
# ]

"""
    configmaps = [
        k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='secret')),
    ]

    Secret = Secret(
        'env',
        'Secret',
        'env2',
        'Secret',
    )

    secrets=[env],
    env_from=configmaps
    ---------------------------------------------------------------------------------
    Dockerfile에서 환경 변수로 설정된 값을 Kubernetes configmap 혹은 secret을 통해 설정된 값을 가져오려면 위와 같이 설정이 필요하다.
    만약 위처럼 설정하지 않는다면 환경 변수로 설정된 값을 호출할 수 없다.
"""
start = DummyOperator(task_id="start", dag=dag) # start

"""
    KubernetesPodOperator를 만들기 위해서는 최소 name, namespace, image, task_id가 필요하다.

    # input json # dag_run.conf trigger

    {"train_img": "416", "train_batch": "2", "train_epochs": "1", "detect_img": "416", "detect_conf": "0.5"} 
    {
        "train": {
            "img": "416",
            "batch": "2",
            "epochs": "1"
        },
        "detect":{
            "img": "416",
            "conf": "0.5"
        }
    }
"""

yolov5_train_kubepod = KubernetesPodOperator(
    task_id="yolov5_train_kubepod", # task ID
    name="yolov5_train_kubepod", # task 이름
    namespace='airflow', # kubernetes내에서 실행할 namespace
    image='jae99c/yolov5-pipeline', # 사용할 도커 이미지
    cmds=["/bin/sh", "-c"], # container 내부에서 실행할 command
    # /bin/sh -c 를 사용하게 되면 ;을 이용해 여러 명령어를 순차 실행할 수 있다.
    # 이때 하나의 쌍 따옴표 아래에 ;으로 명령어들이 구분되어야 한다.
    

    # provide_content=True,
    # env ={'dataset_curl_url': '{{dag_run.conf["dataset_url"]}}'},
    arguments=['python train.py --img {{ dag_run.conf.train_img | d("416") }} \
        --batch {{ dag_run.conf.train_batch | d("2") }} \
        --epochs {{ dag_run.conf.train_epochs | d("1") }} \
        --data /usr/src/app/yolo_pipeline_volume/dataset/data.yaml \
        --cfg ./models/yolov5s.yaml \
        --weights yolov5s.pt \
        --name mask_yolo_result; \
        cp -r /usr/src/app/runs/train/mask_yolo_result /usr/src/app/yolo_pipeline_volume/train_result/mask_yolo_result'], 
        # command에 대한 argument
    
    labels={"foo": "bar"},
    in_cluster=True,
    # secrets=[
    #     env
    # ],
    # image_pull_secrets=[k8s.V1LocalObjectReference('image_credential')], # Public Docker image가 아닌 private Docker image를 가져오려면 image_pull을 할 수 있는 환경 변수 호출을 통해 pull 받아 동작시킬 수 있다. 
    
    is_delete_operator_pod=True, # 포트 삭제 여부 false로 설정하면 PodOperator가 동작하고 pod가 삭제되지 않아 메모리를 점유하고 있을 수 있다.
    get_logs=True, # Airflow 환경에서 Pod 동작 log 출력여부
    resources=pod_resources, #resource 설정
    # env_from=configmaps, #configmap
    startup_timeout_seconds=500, # default timeout은 120초인데, 이미지를 pull받는 시간 동안 초과될 수가 있음.
    volumes=[yolo_volume],
    volume_mounts=[yolo_volume_mount],
    dag=dag,
)

yolov5_detect_kubepod = KubernetesPodOperator(
    task_id="yolov5_detect_kubepod",
    name="yolov5_detect_kubepod",
    namespace='airflow',
    image='jae99c/yolov5-pipeline',
    cmds=["/bin/sh", "-c"],
    arguments=['python detect.py --source /usr/src/app/yolo_pipeline_volume/input/mask.mp4 \
        --weights /usr/src/app/yolo_pipeline_volume/train_result/mask_yolo_result/weights/best.pt \
        --img {{ dag_run.conf.detect_img | d("416") }} \
        --conf {{ dag_run.conf.detect_conf | d("0.5") }}; \
        cp /usr/src/app/runs/detect/exp/mask.mp4 /usr/src/app/yolo_pipeline_volume/detect_result/mask.mp4'], 
    labels={"foo": "bar"},
    in_cluster=True,
    is_delete_operator_pod=True,
    get_logs=True, 
    resources=pod_resources,
    startup_timeout_seconds=500, 
    volumes=[yolo_volume],
    volume_mounts=[yolo_volume_mount],
    dag=dag,
)

upload_to_google_drive_kubepod = KubernetesPodOperator(
    task_id="upload_to_google_drive_kubepod", 
    name="upload_to_google_drive_kubepod",
    namespace='airflow',
    image='jae99c/yolov5-pipeline', 
    cmds=["/bin/sh", "-c"], 
    arguments=['python /usr/src/app/yolo_pipeline_volume/upload_to_google_drive.py' ], 
    labels={"foo": "bar"},
    in_cluster=True,
    is_delete_operator_pod=True, 
    get_logs=True, 
    resources=pod_resources,
    startup_timeout_seconds=500,
    volumes=[yolo_volume],
    volume_mounts=[yolo_volume_mount],
    dag=dag,
)
"""
:: backup ::
    arguments=['python train.py --img 416 --batch 2 --epochs 1 --data /usr/src/app/yolo_pipeline_volume/dataset/data.yaml --cfg ./models/yolov5s.yaml --weights yolov5s.pt --name mask_yolo_result; \
        cp -r /usr/src/app/runs/train/mask_yolo_result /usr/src/app/yolo_pipeline_volume/train_result/mask_yolo_result; \
        python detect.py --source /usr/src/app/yolo_pipeline_volume/input/mask.mp4 --weights /usr/src/app/yolo_pipeline_volume/train_result/mask_yolo_result/weights/best.pt --img 416 --conf 0.5; \
        cp /usr/src/app/runs/detect/exp/mask.mp4 /usr/src/app/yolo_pipeline_volume/detect_result/mask.mp4; \
        python /usr/src/app/yolo_pipeline_volume/upload_to_google_drive.py' ], 
    ---
    curl -L "https://public.roboflow.com/ds/qn6lmo8rhA?key=EkVdFFNjtW" > roboflow.zip; unzip roboflow.zip; rm roboflow.zip;
"""

start >> yolov5_train_kubepod >> yolov5_detect_kubepod >> upload_to_google_drive_kubepod


