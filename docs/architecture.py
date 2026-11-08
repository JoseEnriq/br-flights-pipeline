"""Draw docs/architecture.png, the diagram at the top of the README.

Requires Graphviz and `pip install diagrams==0.25.1`; run from the repository
root: python docs/architecture.py
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Athena, GlueDataCatalog
from diagrams.aws.integration import SimpleNotificationServiceSnsEmailNotification, SNS
from diagrams.aws.security import IAM
from diagrams.aws.storage import S3
from diagrams.generic.storage import Storage
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.ci import GithubActions
from diagrams.onprem.iac import Terraform
from diagrams.onprem.workflow import Airflow
from diagrams.programming.language import Python

GRAPH = {"fontsize": "20", "pad": "0.4", "nodesep": "0.6", "ranksep": "1.2", "splines": "spline"}

with Diagram("br-flights-pipeline", filename="docs/architecture", show=False, direction="LR", graph_attr=GRAPH):
    anac = Storage("ANAC open data\nmonthly flight files")

    with Cluster("Docker, local machine"):
        airflow = Airflow("Airflow\ndaily at 20:00")
        ingest = Python("ingest\nSHA-256 per month")
        dbt = Dbt("dbt build\ntests and reconciliation")

    with Cluster("AWS, us-east-1"):
        user = IAM("pipeline user\nleast privilege")
        with Cluster("S3 data lake"):
            raw = S3("raw CSV\none folder per version")
            processed = S3("processed\nParquet")
            curated = S3("curated\nfact, dimensions, metrics")
        catalog = GlueDataCatalog("Glue Data Catalog")
        athena = Athena("Athena")
        alerts = SNS("SNS topic")

    email = SimpleNotificationServiceSnsEmailNotification("failure email")

    with Cluster("GitHub repository"):
        terraform = Terraform("Terraform\nAWS resources")
        checks = GithubActions("GitHub Actions\ntests and checks")

    # No data flows through the repository; the invisible edge only keeps it
    # next to the local machine instead of alone in a corner
    airflow >> Edge(style="invis") >> [terraform, checks]

    anac >> Edge(label="new or changed files") >> ingest >> raw
    airflow >> [ingest, dbt]
    airflow >> Edge(style="dashed") >> user
    raw >> athena
    athena - Edge(style="dotted") - catalog
    dbt >> Edge(label="SQL on Athena") >> processed >> curated
    airflow >> Edge(label="task failed", color="firebrick") >> alerts >> email
