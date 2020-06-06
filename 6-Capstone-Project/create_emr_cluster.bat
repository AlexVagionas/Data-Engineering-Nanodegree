aws emr create-cluster --name capstone_cluster ^
--use-default-roles --release-label emr-5.29.0  ^
--instance-count 3 --applications Name=Spark Name=Zeppelin  ^
--ec2-attributes KeyName=capstone_project ^
--instance-type m5.xlarge --log-uri s3://emrlogs/