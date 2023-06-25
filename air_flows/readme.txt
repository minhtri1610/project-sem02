#setup
#link hương dẫn cài đặt apache airflow
https://github.com/apache/airflow

# cách cài đặt trên MACOS
# update python neu gap loi
python3.9 -m pip install --upgrade pip

#Khoi tao moi truong python 3
python3 -m venv myenv
source myenv/bin/activate   # Activate the virtual environment

b1: pip3 install 'apache-airflow==2.6.2' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.2/constraints-3.8.txt"

# tao duong dan goc cua airflow tai vi tri thu muc dang dung
export AIRFLOW_HOME=.

# khởi tạo db
airflow db init

# khởi động webserver
airflow webserver -p 8080

# mở file airflow.cfg
tìm tới dòng
sql_alchemy_conn = sqlite:///

thay thế dòng trên bằng sql_alchemy_conn = sqlite:////Users/IntelTri/WorkPlace/Aptech/Project-sem2/project-sem02/air_flows/airflow.db

airflow db reset
airflow db init

# khởi động lai webserver
airflow webserver -p 8080

# truy cap vao web bang duong dan
http://0.0.0.0:8080

# tao user and pass
airflow users create --username alphateam --firstname admin_alpha_data --lastname team --role Admin --email epsminhtri@gmail.com

#nhap pass:
admin123

# khoi dong airflow tren macos
source myenv/bin/activate
airflow webserver -p 8080


#DAG -> directed Acyclic Graph

#cau hinh lai duong dan tai file airflow.cfg

dags_folder = /Users/IntelTri/WorkPlace/Aptech/Project-sem2/project-sem02/air_flows
airflow db reset

#link thu muc goc ve thu muc hien tai
/Users/IntelTri/airflow
ln -s /Users/IntelTri/WorkPlace/Aptech/Project-sem2/project-sem02/air_flows /Users/IntelTri/airflow
/Users/IntelTri/WorkPlace/Aptech/Project-sem2/project-sem02/air_flows

unlink /Users/IntelTri/airflow

airflow tasks kill <dag_id> <task_id> <execution_date>

https://airflow.apache.org/docs/apache-airflow/1.10.2/scheduler.html

airflows dags list

airflow tasks run tri_test_02 run_etl 2023-06-25

# phan insert datamarts, quarter va month dang day gia tri null

Cac buoc chay

- viet file python
- chay airflow webserver -p 8080
- run triger dags
- airflow schedule

