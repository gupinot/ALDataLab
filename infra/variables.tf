variable "key_name" {
    description = "Name of the SSH keypair to use in AWS."
}

variable "key_path" {
    description = "Path to the private portion of the SSH key specified."
}

variable "aws_region" {
    description = "AWS region to launch servers."
}

variable "aws_availability_zone" {
    description = "AWS availability zone to use."
}

variable "aws_access_key" {
    decscription = "AWS Access Key"
}

variable "aws_secret_key" {
    description = "AWS Secret Key"
}

variable "instance_type" {
    description = "Instance type"
}

variable "spot_price" {
    description = "Max price for spot request"
}

variable "aws_amis" {
    default = {
        "us-east-1" = "ami-6726710d"
    }
}
