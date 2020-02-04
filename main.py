import pandas as pd
import local_v3io_frames as v3f
from datetime import datetime


def init_context(context):
    # client = v3f.Client("https://framesd.default-tenant.app.dev65.lab.iguazeng.com", container="users",token="3452431e-30a8-42eb-b381-afc0dc3f579b")
    client = v3f.Client("http://framesd:8080", container="users", token="3452431e-30a8-42eb-b381-afc0dc3f579b", persist_connection=True)
    # client = v3f.Client("grpc://framesd:8081", container="users", token="3452431e-30a8-42eb-b381-afc0dc3f579b", persist_connection=True)
    setattr(context.user_data, 'frames_client', client)


def handler_good(context, event):
    """
    Reusing the frames client
    """
    df = pd.DataFrame(data={'cpu': 123}, index=[datetime.now()])

    # reusing client
    context.user_data.frames_client.write('tsdb', 'avi', df, labels={'something': context.worker_id})
    return ""


def handler_bad(context, event):
    """
    Not reusing the frames client
    """
    df = pd.DataFrame(data={'cpu': 123}, index=[datetime.now()])

    # client = v3f.Client("https://framesd.default-tenant.app.dev65.lab.iguazeng.com", container="users",token="3452431e-30a8-42eb-b381-afc0dc3f579b")
    client = v3f.Client("http://framesd:8080", container="users", token="3452431e-30a8-42eb-b381-afc0dc3f579b")
    # client = v3f.Client("grpc://framesd:8081", container="users", token="3452431e-30a8-42eb-b381-afc0dc3f579b")
    client.write('tsdb', 'avi', df, labels={'something': context.worker_id})
    return ""
