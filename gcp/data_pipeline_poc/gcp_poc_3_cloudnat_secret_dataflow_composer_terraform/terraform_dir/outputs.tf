output "sa-email" {
	value = "${google_service_account.service_account.email}"
	description = "The created service account email"
}

output "vpc_name" {
	value = "$(google_compute_network.vpc.id}" 
	description = "The created vpc"
}