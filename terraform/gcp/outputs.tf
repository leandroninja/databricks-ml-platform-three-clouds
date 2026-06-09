output "gcs_bucket_name" {
  description = "Nome do bucket GCS do lakehouse"
  value       = google_storage_bucket.lakehouse.name
}

output "gcs_bucket_url" {
  description = "URL gs:// do bucket"
  value       = "gs://${google_storage_bucket.lakehouse.name}"
}

output "vpc_network_name" {
  description = "Nome da VPC network"
  value       = google_compute_network.databricks.name
}

output "subnet_name" {
  description = "Nome da subnet do Databricks"
  value       = google_compute_subnetwork.databricks.name
}

output "databricks_service_account_email" {
  description = "Email da Service Account do Databricks"
  value       = google_service_account.databricks.email
}

output "workload_identity_pool_name" {
  description = "Nome do Workload Identity Pool"
  value       = google_iam_workload_identity_pool.databricks.name
}

output "workload_identity_provider_name" {
  description = "Nome do Workload Identity Pool Provider"
  value       = google_iam_workload_identity_pool_provider.databricks.name
}
