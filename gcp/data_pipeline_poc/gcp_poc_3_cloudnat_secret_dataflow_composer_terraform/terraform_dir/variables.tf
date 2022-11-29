variable "project_id" {
	type = string
}

variable "project_number" {
	type = string
}

variable "region" {
	type = string
}

variable "deletion_protection" {
	type = string
}

variable "gcp_service_list" {
	description = "The list of apis necessary for the project" 
	type = list(string)
	default = ["dataflow.googleapis.com", # cloud dataflow 
				"composer.googleapis.com", # cloud composer 
				"secretmanager.googleapis.com" # secret manager	
	]
}

# IAM Variables
variable "sa_id" {
	type = string
}

# VPC Network
variable "vpc_name" {
	type string
}

variable "subnetwork_name" {
	type = string
}

variable "subnetwork_ip_range" {
	type = string
}

variable "subnetwork_router" {
	type = string
}

# Composer
variable "composer name" {
	type = string
}

variable "composer_image_version" {
	type = string
}