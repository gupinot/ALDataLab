provider "aws" {
    region = "${var.aws_region}"
    access_key = "${var.aws_access_key}"
    secret_key = "${var.aws_secret_key}"
}

resource "aws_spot_instance_request" "influxdb" {
  connection {
    user = "ubuntu"
    key_file = "${var.key_path}"
  }

  instance_type = "${var.instance_type}"
  availability_zone = "${var.aws_availability_zone}"
  ami = "${lookup(var.aws_amis, var.aws_region)}"
  iam_instance_profile="s3_read_only"
  key_name = "${var.key_name}"

  spot_price = "${var.spot_price}"
  wait_for_fulfillment = "true"

  provisioner "local-exec" {
     command = "echo  ${aws_spot_instance_request.influxdb.public_ip} ansible_connection=ssh ansible_ssh_user=ubuntu ansible_ssh_private_key_file=${var.key_path} > ansible/hosts/influxdb.host "
  }

  tags {
    "Name" = "influxdb"
  }
}
