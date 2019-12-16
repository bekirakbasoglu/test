from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Following are defaults which can be overridden later on
default_args = {
    "owner": 'airflow',
    "start_date": days_ago(1)
}

dag = DAG(dag_id='test1',
          description="Simple_Test_DAG",
          default_args=default_args,
          schedule_interval=None,
          catchup=False)


def print_world():
    print('world!!!')


# t1, t2, t3 and t4 are examples of tasks created using operators
t1 = BashOperator(task_id='task_1', bash_command='echo "Hello World from Task 1"', dag=dag)
t2 = BashOperator(task_id='task_2', bash_command='echo "Hello World from Task 2"', dag=dag)
t3 = BashOperator(task_id='task_3', bash_command='echo "Hello World from Task 3"', dag=dag)
t4 = BashOperator(task_id='task_4', bash_command='echo "Hello World from Task 4"', dag=dag)
t5 = PythonOperator(task_id='task_5', python_callable=print_world, dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
