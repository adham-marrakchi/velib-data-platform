# =============================================================
# EC2 Instances (optional - for non-K8s deployments)
# =============================================================

# Look up latest Amazon Linux 2023 AMI
data "aws_ami" "amazon_linux" {
  count       = var.deploy_ec2 ? 1 : 0
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "airflow" {
  count                  = var.deploy_ec2 ? 1 : 0
  ami                    = var.ami_id != "" ? var.ami_id : data.aws_ami.amazon_linux[0].id
  instance_type          = var.instance_type
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null
  subnet_id              = aws_subnet.private[0].id
  vpc_security_group_ids = [aws_security_group.pipeline.id]

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = <<-EOF
    #!/bin/bash
    yum update -y
    yum install -y docker
    systemctl start docker
    systemctl enable docker
    usermod -aG docker ec2-user
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
  EOF

  tags = {
    Name = "${var.project_name}-airflow"
    Role = "orchestration"
  }
}

resource "aws_instance" "kafka" {
  count                  = var.deploy_ec2 ? 1 : 0
  ami                    = var.ami_id != "" ? var.ami_id : data.aws_ami.amazon_linux[0].id
  instance_type          = var.instance_type
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null
  subnet_id              = aws_subnet.private[0].id
  vpc_security_group_ids = [aws_security_group.pipeline.id]

  root_block_device {
    volume_size = 100
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "${var.project_name}-kafka"
    Role = "streaming"
  }
}

resource "aws_instance" "spark" {
  count                  = var.deploy_ec2 ? 1 : 0
  ami                    = var.ami_id != "" ? var.ami_id : data.aws_ami.amazon_linux[0].id
  instance_type          = "t3.xlarge"
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null
  subnet_id              = aws_subnet.private[0].id
  vpc_security_group_ids = [aws_security_group.pipeline.id]

  root_block_device {
    volume_size = 100
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "${var.project_name}-spark"
    Role = "processing"
  }
}
