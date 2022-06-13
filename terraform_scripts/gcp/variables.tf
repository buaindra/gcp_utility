variable "project_id" {
  type = string
  default = "asset-tracker-347111"
}

variable "sa_id" {
  type = string
}

variable "bucket_name" {
  type = string
  description = "Bucket name"
}

variable "bucket_location" {
  type = string
  default = "asia-south1"
}

variable "bucket_storage_class" {
  type = string
  default = "REGIONAL"
}

variable "cloudsql_name" {
  type = string
}

variable "cloudsql_database_version" {
  type = string
}

variable "cloudsql_region" {
  type = string
}

variable "cloudsql_machine_type" {
  type = string
}