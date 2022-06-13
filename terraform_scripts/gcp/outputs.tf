output "sa-email" {
  value       = "${google_service_account.service_account.email}"
  description = "The created network"
}