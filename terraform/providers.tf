terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.25"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
  }

  # Uncomment and configure for remote state (recommended for teams)
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "e2e-pipeline/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "e2e-data-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

provider "kubernetes" {
  host                   = var.deploy_to_eks ? aws_eks_cluster.eks[0].endpoint : null
  cluster_ca_certificate = var.deploy_to_eks ? base64decode(aws_eks_cluster.eks[0].certificate_authority[0].data) : null
  token                  = var.deploy_to_eks ? data.aws_eks_cluster_auth.eks[0].token : null

  config_path = var.deploy_to_eks ? null : "~/.kube/config"
}

provider "helm" {
  kubernetes {
    host                   = var.deploy_to_eks ? aws_eks_cluster.eks[0].endpoint : null
    cluster_ca_certificate = var.deploy_to_eks ? base64decode(aws_eks_cluster.eks[0].certificate_authority[0].data) : null
    token                  = var.deploy_to_eks ? data.aws_eks_cluster_auth.eks[0].token : null

    config_path = var.deploy_to_eks ? null : "~/.kube/config"
  }
}

data "aws_eks_cluster_auth" "eks" {
  count = var.deploy_to_eks ? 1 : 0
  name  = aws_eks_cluster.eks[0].name
}
