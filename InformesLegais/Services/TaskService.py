from bson import ObjectId

from InformesLegais.models import TaskModel
from .ServiceBase import ServiceBase
from datetime import datetime
from uuid import UUID
from bson import Binary
import pandas as pd

class TaskService(ServiceBase):

    def get_all_tasks(self):
        with self.app.app_context():
            tasks = self.db.tasks.find({})
            df = pd.DataFrame.from_dict(tasks)
            return df.to_dict('records')


    def start_task(self ,nome ):

        with self.app.app_context():
            task = TaskModel( name=nome, status="Iniciada" ,
                             start_time=datetime.now() , end_time=datetime.now()  )

            task2 = self.db.tasks.insert_one(task.to_mongo())

            return str(task.id)


    def finish_task(self , task_id):

        with self.app.app_context():
            try:
                task = self.db.tasks.find_one_and_update(
                    {"id": Binary.from_uuid(UUID(task_id))},
                    {"$set": {"status": "Concluido" , 'end_time':datetime.now()}},
                )
                return task
                print ("tarefa atualizada com sucesso")
            except Exception as e:
                print (f"Erro ao atualizar {e}")



