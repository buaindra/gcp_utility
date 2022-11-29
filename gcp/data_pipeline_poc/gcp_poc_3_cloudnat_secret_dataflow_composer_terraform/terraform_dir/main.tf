provider "google" {
	project = var.project_id
	region = var.region
}

# enable google cloud services
resource "google_project_service" "gcp_services" { 
	for_each = toset(var.gcp_service_list)
	project = var.project_id 
	service = each.key

	provisioner "local-exec" { 
		command = "sleep 5"
	}
}

#create service account
resource "google_service_account" "service_account" {
	account_id = var.sa_id
	display_name = var.sa_id
	description = "created this sa for both composer, dataflow jobs"

	provisioner "local-exec" {
		command = "sleep 5"
	}
	depends_on = [google_project_service.gcp_services]
}

# Provide IAM role binding withthe newly created service account
resource "google_project_iam_binding" "sa_role_dataflow_admin" {
	project = var.project_id 
	role = "roles/dataflow.admin"
	members = [ 
		"serviceAccount:${google_service_account.service_account.email}"
	]
	depends_on = [google_service_account.service_account]
}

resource "google_project_iam_binding" "sa_role_dataflow_worker" { 
	project = var.project_id 
	role = "roles/dataflow.worker"
	members = [
		"serviceAccount:${google_service_account.service_account.email}"
	]
	depends_on = [google_service_account.service_account]
}

resource "google_project_iam_binding" "sa_role_secretmanager_secretAccessor" {
	project = var.project_id 
	role = "roles/secretmanager.secretAccessor"
	members = [
		"serviceAccount:${google_service_account.service_account.email}"
	]
	depends_on = [google_service_account.service_account] 
}

resource "google_project_iam_binding" "sa_role_secretmanager_bigquery_admin" { 
	project = var.project_id
	role = "roles/bigquery.admin"
	members = [
		"serviceAccount:${google Tervice_account.service_account.email}"
	]
	depends_on = [google_service_account.service_account]
}

resource "google_project_iam_member" "storage_object_admin" {
	project var.project_id
	role = "roles/storage.objectAdmin" 
	member = "serviceAccount:${google_service_account.service account.email}"
	depends_on = [google_service_account.service_account]
}

resource "google_project_iam binding" "sa_role_service_account_user" { 
	project = var.project_id
	role = "roles/iam.serviceAccountUser"
	members = [
		"serviceAccount:${google_service_account.service_account.email}"
	]
	depends_on = [google_service_account.service_account]
}

resource "google_project_iam_member "composer-worker" {
	project = var.project_id 
	role = "roles/composer.worker"
	member = "serviceAccount:${google_service_account.service_account.email}"
	depends_on  = [google_service_account.service_account]
}

# create vpc network
resource "google_compute_network" "vpc" {
	name = var.vpc_name
	auto_create_subnetworks = false 
	depends_on =[google_service_account.service_account]
}

# create subnetwork on new vpc
resource "google_compute_subnetwork" "subnetwork" {
	name = var.subnetwork_name
	ip_cidr_range = var.subnetwork_ip_range
	region = var.region
	network = google_compute_network.vpc.id
	private_ip_google_access = true
	depends_on = [google_compute_network.vpc]
}

resource "google_compute_router" "router" { 
	name = var.subnetwork_router
	region = google_compute_subnetwork.subnetwork.region
	network = google_compute_network.vpc.id

	#bgp {
	#	asn 64514
	#}
	
	depends_on = [google_compute subnetwork.subnetwork]
}

resource "google_compute_address" "address" {
	count = 1
	name = "nat-manual-ip-${count.index}"
	region = google_compute_subnetwork.subnetwork.region

	depends_on = [google_compute_subnetwork.subnetwork]
}

#Cloud Nat 
#https://stackoverflow.com/questions/56773692/terraform-google-cloud-nat-using-reserved-static-ip

resource "google_compute_router_nat" "nat" (
	name = "pipeline-router-nat"
	router = google_compute_router.router.name
	region = google_compute router.router.region
	nat_ip_allocate_option = "MANUAL_ONLY"  #"AUTO_ONLY
	nat_ips = "${google_compute_address.address.*.self_link}" 
	source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

	subnetwork {
		name = "${google_compute_subnetwork.subnetwork.id}"
		source_ip_ranges_to_nat = ["ALL_IP_RANGES"]

	log_config {
		enable = true
		filter = "ERRORS_ONLY"
	}
	
	depends_on = [google_compute_router.router]
}

#create cloud composer --start--

resource "google_project_lam member" "composer-service-agent" {
	project = var.project_id 
	role = "roles/composer.ServiceAgentV2Ext"  #this is require for composer version 2
	member = "serviceAccount:service-$(var.project_number)@cloudcomposer-accounts.iam.gserviceaccount.com"
	depends_on = [google service account.service account]
}

resource "google_composer_environment" "composer_env" {
	name = var.composer_name
	region = var.region 
	config {
		software_config {
			image_version = var.composer_image_version
			
			env_variables = {
				project_id = var.project_id
				region = var.region
				subnetwork = google_compute_subnetwork.subnetwork.name
				service_account_email = "${var.sa_id}@${var.project_id}.iam.gserviceaccount.com"
				#AIRFLOW_VAR_vars_json = "{'sample_key': 'sample_val'}"
			}
		}
		
		workloads_config {
			scheduler {
				cpu = 0.5 
				memory_gb = 1.875
				storage_gb = 1
				count = 1
			}
			
			web_server { 
				cpu = 0.5
				memory_gb = 1.875
				storage_gb = 1
			}

			worker {
				cpu = 0.5
				memory_gb = 1.875
				storage_gb = 1
				min_count = 1
				max_count = 3
			}
		}
		environment_size = "ENVIRONMENT_SIZE_SMALL"

		node_config {
			#network = google_compute_network.vpc.id
			#subnetwork = google_compute_subnetwork.subnetwork.id
			service_account = google_service_account.service_account.name
		}
	}

	depends_on = [google_project_iam member.composer-service-agent, 
		google_project_iam member.composer-worker]
}