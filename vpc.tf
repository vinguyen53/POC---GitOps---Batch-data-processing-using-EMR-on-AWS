#Create vpc for emr
resource "aws_vpc" "emr-vpc" {
  cidr_block = "10.1.0.0/16"
  tags = {
    Name = "emr-vpc"
  }
}

#Create subnet for emr
resource "aws_subnet" "emr-subnet" {
  vpc_id     = aws_vpc.emr-vpc.id
  cidr_block = "10.1.1.0/24"
  availability_zone = "ap-southeast-1a"
  tags = {
    Name = "emr-subnet"
  }
}

#Create master security group for emr
resource "aws_security_group" "emr-master-sg" {
  name = "emr-master-sg"
  vpc_id = aws_vpc.emr-vpc.id

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
  }

}

resource "aws_security_group_rule" "emr-master-sg-rule" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "TCP"
  cidr_blocks       = ["3.0.5.32/29"]
  security_group_id = aws_security_group.emr-master-sg.id
  depends_on = [ aws_security_group.emr-master-sg ]
}

#Create S3 vpc endpoint
resource "aws_vpc_endpoint" "s3-endpoint" {
  vpc_id = aws_vpc.emr-vpc.id
  service_name = "com.amazonaws.ap-southeast-1.s3"
}

#add vpc endpoint to route table
resource "aws_vpc_endpoint_route_table_association" "emr-vpc-endpoint-route" {
  route_table_id  = aws_vpc.emr-vpc.main_route_table_id
  vpc_endpoint_id = aws_vpc_endpoint.s3-endpoint.id
  
}

#Create internet gateway
resource "aws_internet_gateway" "emr-vpc-internet-gateway" {
  vpc_id = aws_vpc.emr-vpc.id
}

#add internet gateway to route table
resource "aws_route" "emr-vpc-igw-route" {
  route_table_id = aws_vpc.emr-vpc.main_route_table_id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id = aws_internet_gateway.emr-vpc-internet-gateway.id
}