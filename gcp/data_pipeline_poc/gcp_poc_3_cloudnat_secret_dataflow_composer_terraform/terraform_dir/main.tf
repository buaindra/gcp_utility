provider "google" {

= var.project_id

project region = var.region

# enable google cloud services

resource "google_project_service" "gcp_services" { for each toset (var.gcp_service_list)

project var.project_id service each.key

provisioner "local-exec" { command = "sleep 5"

}

}

#create service account

resource "google_service_account" "service_account" {

account_id var.sa_id

display_name = var.sa_id

description = "created this sa for both composer, dataflow jobs"

provisioner "local-exec" {

command = "sleep 5"

depends on = [google_project_service.gcp_services]

I count # Provide IAM role binding withthe newly created service account

resource "google_project_iam_binding" "sa_role_dataflow_admin" {

project = var.project_id role = "roles/dataflow.admin"

members = [ "serviceAccount:${google_service_account.service_account.email)"

depends on [google_service_account.service_account]

resource "google_project_iam_binding" "sa_role_dataflow_worker" { project = var.project_id role = "roles/dataflow.worker"

members = [

"serviceAccount:${google_service_account.service_account.email)"

depends on = [google_service_account.service_account]

resource "google_project_iam_binding" "sa_role_secretmanager_secretAccesso

project = var.project_id role = "roles/secretmanager.secretAccessor"

members = [

"serviceAccount:$(google_service_account.service_account.email}"

depends on = [google_service_account.service_account] }

resource "google_project_iam_binding" "sa_role_secretmanager_bigquery_admin project = var.project_id

role "roles/bigquery.admin"

members = [

"serviceAccount:${google Tervice_account.service_account.email}"

]

depends on = [google_service_account.service_account]

resource "google_project_iam_member" "storage_object_admin" {

project var.project_id

role = "roles/storage.objectAdmin" member = "serviceAccount:$(google_service_account.service account.email)" depends on [google_service_account.service_account]

resource "google_project_iam binding" "sa_role_service_account_user" { project = var.project_id

role ="roles/iam.serviceAccountUser"

members - [

"serviceAccount:${google_service_account.service_account.email)"

depends on [google_service_account.service_account]

resource "google_project_iam member "composer-worker" {

project var.project_id role -roles/composer.worker"

member = "serviceAccount:$(google_service_account.service_account.email}"

depends on [google_service_account.service_account]

# create vpc network

resource "google_compute_network" "vpc" {

name var.vpc_name

auto_create_subnetworks false depends on [google_service_account.service_account]

name

# create subnetwork on new vpc

resource "google_compute_subnetwork"

"subnetwork" {

var.subnetwork_name

ip_cidr_range var.subnetwork_ip_range

region

= var.region

network

google_compute network.vpc.id

private_ip_google_access true

depends on [google_compute_network.vpc]

name region google_compute_subnetwork.subnetwork.region

resource "google_compute_router" "router" { van.subnetwork_router

network google_compute_network.vpc.id

#bgp

asn 64514

depends on [google_compute subnetwork.subnetwork]

-1

resource "google compute_address" "address"

{

count

"nat-manual-ip-$(count.index)"

region - google_compute subnetwork.subnetwork.region.

name

depends on [google_compute_subnetwork.subnetwork]

#Cloud Nat #https://stackoverflow.com/questions/56773692/terraform-google-cloud-nat-using-reserved-static-ip

name

resource "google compute router nat" "nat" (

pipeline router-nat"

router

google_compute_router.router.name

region

- google_compute router.router region

nat ip_allocate_option nat ips

I

"MANUAL ONLY" "AUTO ONLY

"${google_compute_address.address.".self link}" = "LIST OF SUBNETWORKS"

source_subnetwork_ip_ranges_to_nat

name

subnetwork {

"$(google_compute subnetwork, subnetwork.id}"

source_ip_ranges to nat "ALL IP_RANGES"]

log config (

enable true

filter "ERRORS ONLY"

depends on [google_compute_router.router]

#create cloud composer-start

resource "google_project_lam member "composer-service-agent" {

project var.project id -"roles/composer ServiceAgentV2Ext this is require for composer version 2

role

"serviceAccount:service-$(var.project_number)cloudcomposer-accounts.lam.gserviceaccount.com

depends on [google service account.service account]

resource

"google_composer environment composer env" [

hame -var.composer_name

region var.region config

software config (

image version var.composer_image_version

env variables t

project_id var.project_id

region var-region

subnetwork google_compute_subnetwork.subnetwork.name

service account_email"$(var.sa_id)@s (var.project_id). 1am.gserviceaccount.com

BAIRFLOW VAR vars 1son (sample_key: sample_val"}

I

}

workloads config ( (

scheduler cpu

-0.5 memory gb 1.875

storage gb 1

count

1

web server { cpu -0.5

memory gb 1.875

storage gb 1

worker (
cpu 0.59

memory_gb 1.875 storage_gb = 1

min_count = 1

}

max_count = 3

}

environment_size = "ENVIRONMENT_SIZE_SMALL"

node_config {

#network google_compute_network.vpc.id

#subnetwork = google_compute_subnetwork.subnetwork.id

service account google_service_account.service_account.name

=

}

depends on [google_project_iam member.composer-service-agent, google_project_iam member.composer-worker]