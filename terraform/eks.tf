# =============================================================
# EKS Cluster & Node Group
# =============================================================

# IAM Role for EKS Cluster
resource "aws_iam_role" "eks_role" {
  count = var.deploy_to_eks ? 1 : 0
  name  = "${var.project_name}-eks-cluster-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "eks.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  count      = var.deploy_to_eks ? 1 : 0
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.eks_role[0].name
}

resource "aws_iam_role_policy_attachment" "eks_vpc_controller" {
  count      = var.deploy_to_eks ? 1 : 0
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController"
  role       = aws_iam_role.eks_role[0].name
}

# EKS Cluster
resource "aws_eks_cluster" "eks" {
  count    = var.deploy_to_eks ? 1 : 0
  name     = var.eks_cluster_name
  role_arn = aws_iam_role.eks_role[0].arn
  version  = "1.28"

  vpc_config {
    subnet_ids              = aws_subnet.private[*].id
    endpoint_private_access = true
    endpoint_public_access  = true
    security_group_ids      = [aws_security_group.eks[0].id]
  }

  enabled_cluster_log_types = ["api", "audit", "authenticator"]

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
    aws_iam_role_policy_attachment.eks_vpc_controller,
  ]

  tags = {
    Name = var.eks_cluster_name
  }
}

# IAM Role for EKS Worker Nodes
resource "aws_iam_role" "eks_nodes" {
  count = var.deploy_to_eks ? 1 : 0
  name  = "${var.project_name}-eks-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_worker_node" {
  count      = var.deploy_to_eks ? 1 : 0
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_nodes[0].name
}

resource "aws_iam_role_policy_attachment" "eks_cni" {
  count      = var.deploy_to_eks ? 1 : 0
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_nodes[0].name
}

resource "aws_iam_role_policy_attachment" "eks_ecr" {
  count      = var.deploy_to_eks ? 1 : 0
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_nodes[0].name
}

# EKS Node Group
resource "aws_eks_node_group" "eks_nodes" {
  count           = var.deploy_to_eks ? 1 : 0
  cluster_name    = aws_eks_cluster.eks[0].name
  node_group_name = "${var.project_name}-nodes"
  node_role_arn   = aws_iam_role.eks_nodes[0].arn
  instance_types  = [var.eks_node_instance_type]
  subnet_ids      = aws_subnet.private[*].id
  disk_size       = 50

  scaling_config {
    desired_size = var.eks_node_desired
    min_size     = var.eks_node_min
    max_size     = var.eks_node_max
  }

  labels = {
    role = "pipeline-worker"
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node,
    aws_iam_role_policy_attachment.eks_cni,
    aws_iam_role_policy_attachment.eks_ecr,
  ]

  tags = {
    Name = "${var.project_name}-node-group"
  }
}
