provider "google" {
}

#1st create project and provide basic compute engine permission to your user

#create service account
resource "google_service_account" "service_account" {
  account_id   = var.sa_id
  display_name = var.sa_id
  description = "created this sa for testing terraform"
  project = var.project_id
  
  provisioner "local-exec" {
    command = "sleep 5"
  }
}

# Provide IAM role binding withthe newly created service account
resource "google_project_iam_binding" "sa_role_1" {
  project = var.project_id
  role    = "roles/storage.admin"
  members = [
    "serviceAccount:${google_service_account.service_account.email}"
  ]
  depends_on = [google_service_account.service_account]
}

resource "google_project_iam_binding" "sa_role_2" {
  project = var.project_id
  role    = "roles/serviceusage.serviceUsageAdmin"
  members = [
    "serviceAccount:${google_service_account.service_account.email}"
  ]
  depends_on = [google_service_account.service_account]
}


# create storage bucket for static and media files
resource "google_storage_bucket" "default" {
  name = var.bucket_name
  project = var.project_id
  storage_class = var.bucket_storage_class
  location = var.bucket_location
}

# create cloudsql with mysql
resource "google_sql_database_instance" "main" {
  name = var.cloudsql_name
  project = var.project_id
  database_version = var.cloudsql_database_version
  region = var.cloudsql_region
  deletion_protection = "false"
  
  settings {
    tier = var.cloudsql_machine_type
  }
}