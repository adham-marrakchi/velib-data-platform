# =============================================================
# Outputs
# =============================================================

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = var.deploy_to_eks ? aws_eks_cluster.eks[0].name : null
}

output "eks_cluster_endpoint" {
  description = "EKS cluster API endpoint"
  value       = var.deploy_to_eks ? aws_eks_cluster.eks[0].endpoint : null
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = aws_db_instance.rds_postgresql.endpoint
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.rds_postgresql.db_name
}

output "s3_bucket_name" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "s3_bucket_arn" {
  description = "S3 data lake bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "security_group_pipeline" {
  description = "Pipeline security group ID"
  value       = aws_security_group.pipeline.id
}
