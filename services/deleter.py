import os

class Deleter:
    def delete(self, **kwargs):
        ti = kwargs['ti']
        filepathes = ti.xcom_pull(task_ids='run_push_from_disk', key='filepathes to remove')
        for filepath in filepathes:
            try:
                os.remove(filepath)
            except:
                continue
