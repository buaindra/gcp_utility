project_id = "sappi-toc-sbx"
project_number = "526545899170" 
region = "europe-west3"
deletion_protection = "false"

# service account
sa_id = "sa-gcp-pipeline"

#network
vpc_name = "vpc-network-datapipeline" 
subnetwork_name = "sub-network-datapipeline"
subnetwork_ip_range="10.2.0.0/16"
subnetwork_router = "sub-network-router"

# cloud composer
composer_name = "composer-env"
composer_image_version = "composer-2.0.22-airflow-2.2.5"