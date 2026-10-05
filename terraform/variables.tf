# =============================================================
# General
# =============================================================
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project name used for resource naming and tagging"
  type        = string
  default     = "e2e-pipeline"
}

# =============================================================
# Networking
# =============================================================
variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24"]
}

variable "allowed_ssh_cidrs" {
  description = "CIDR blocks allowed to SSH (restrict to your IP in prod)"
  type        = list(string)
  default     = ["0.0.0.0/0"] # CHANGE THIS for production
}

# =============================================================
# Compute
# =============================================================
variable "instance_type" {
  description = "EC2 instance type for pipeline components"
  type        = string
  default     = "t3.large"
}

variable "ami_id" {
  description = "AMI ID for EC2 instances (Amazon Linux 2023)"
  type        = string
  default     = "" # Set via tfvars or data source
}

variable "key_pair_name" {
  description = "Name of the EC2 key pair for SSH access"
  type        = string
  default     = ""
}

variable "deploy_ec2" {
  description = "Whether to deploy EC2 instances (set false if using EKS only)"
  type        = bool
  default     = false
}

# =============================================================
# EKS
# =============================================================
variable "deploy_to_eks" {
  description = "Whether to deploy EKS cluster"
  type        = bool
  default     = true
}

variable "eks_cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "e2e-pipeline"
}

variable "eks_node_instance_type" {
  description = "Instance type for EKS worker nodes"
  type        = string
  default     = "t3.large"
}

variable "eks_node_desired" {
  description = "Desired number of EKS worker nodes"
  type        = number
  default     = 2
}

variable "eks_node_min" {
  description = "Minimum number of EKS worker nodes"
  type        = number
  default     = 1
}

variable "eks_node_max" {
  description = "Maximum number of EKS worker nodes"
  type        = number
  default     = 4
}

# =============================================================
# Database (RDS)
# =============================================================
variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.medium"
}

variable "db_username" {
  description = "Database master username"
  type        = string
  default     = "pipeline_admin"
}

variable "db_password" {
  description = "Database master password"
  type        = string
  sensitive   = true
}

variable "db_allocated_storage" {
  description = "Allocated storage in GB for RDS"
  type        = number
  default     = 50
}

variable "db_multi_az" {
  description = "Enable Multi-AZ for RDS (recommended for prod)"
  type        = bool
  default     = false
}

# =============================================================
# Deployment Strategy
# =============================================================
variable "enable_blue_green" {
  description = "Enable blue/green deployment strategy"
  type        = bool
  default     = true
}

variable "enable_canary" {
  description = "Enable canary deployment strategy"
  type        = bool
  default     = true
}

variable "canary_initial_weight" {
  description = "Initial traffic weight for canary deployments (%)"
  type        = number
  default     = 10
}

variable "enable_monitoring" {
  description = "Enable Prometheus and Grafana monitoring"
  type        = bool
  default     = true
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for deployment notifications"
  type        = string
  default     = ""
  sensitive   = true
}
