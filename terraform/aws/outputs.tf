output "workspace_url" {
  description = "URL do workspace Databricks na AWS"
  value       = databricks_mws_workspaces.main.workspace_url
}

output "workspace_id" {
  description = "ID do workspace Databricks"
  value       = databricks_mws_workspaces.main.workspace_id
}

output "s3_bucket_name" {
  description = "Nome do bucket S3 do lakehouse"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "s3_bucket_arn" {
  description = "ARN do bucket S3"
  value       = aws_s3_bucket.lakehouse.arn
}

output "vpc_id" {
  description = "ID da VPC do Databricks"
  value       = aws_vpc.main.id
}

output "instance_profile_arn" {
  description = "ARN do Instance Profile para usar nos clusters"
  value       = aws_iam_instance_profile.cluster.arn
}

output "cross_account_role_arn" {
  description = "ARN da cross-account role do Databricks"
  value       = aws_iam_role.databricks_cross_account.arn
}

output "private_subnet_ids" {
  description = "IDs das subnets privadas"
  value       = [aws_subnet.private_az1.id, aws_subnet.private_az2.id]
}

output "databricks_token" {
  description = "Token de acesso ao workspace (gerado pelo Terraform — rotate após o primeiro uso)"
  value       = databricks_mws_workspaces.main.token[0].token_value
  sensitive   = true
}
