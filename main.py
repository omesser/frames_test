import pandas as pd
import local_v3io_frames as v3f
from datetime import datetime


def handler(context, event):
    df = pd.DataFrame(data={'cpu': 123}, index=[datetime.now()])

    # client = v3f.Client("https://framesd.default-tenant.app.dev65.lab.iguazeng.com", container="users",token="3452431e-30a8-42eb-b381-afc0dc3f579b")
    client = v3f.Client("http://framesd:8080", container="users", token="3452431e-30a8-42eb-b381-afc0dc3f579b")
    client.write('tsdb', 'avi', df, labels={'something': context.worker_id})
    return ""
