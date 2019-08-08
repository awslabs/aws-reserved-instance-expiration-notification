# Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import boto3
from botocore.exceptions import ClientError
from multiprocessing import Process
from dateutil import relativedelta
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
import os

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

TARGET_REGION = 'ap-northeast-2'
SES_REGION = 'us-east-1'
SENDER = "blah <your_email@example.com>"

def send_email(info):
    RECIPIENT = info['email']
    attachments = info['attach']
    BODY_HTML = info['msg']

    message = MIMEMultipart()
    message['Subject'] = 'Amazon RI Expiration Notification'
    message['From'] = SENDER
    message['To'] = RECIPIENT
    destinations = []
    destinations.append(RECIPIENT)

    # message body
    part = MIMEText(BODY_HTML, 'html')
    message.attach(part)

    # attachment
    for attachment in attachments:
        with open(attachment, 'rb') as f:
            part = MIMEApplication(f.read())
            part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
            message.attach(part)

    client = boto3.client('ses', region_name=SES_REGION)
    response = client.send_raw_email(
        Source=message['From'],
        Destinations=destinations,
        RawMessage={
            'Data': message.as_string()
        }
    )


def getExpRIList(response: boto3.resources.response, response_name, filter_name, filter_value, select_column=[]):
    if len(response[response_name]) > 0:
        print("Exist " + response_name)

    next_month = np.datetime64(datetime.utcnow() + relativedelta.relativedelta(days=31))
    ri_list = list(map(lambda a: a.values(), filter(lambda a: a[filter_name] == filter_value, response[response_name])))
    head = response[response_name][0].keys()
    df = pd.DataFrame(ri_list, columns=head)
    df['StartTime'] = pd.to_datetime(df['StartTime']).dt.tz_convert(None)
    df['End'] = df['StartTime'] + pd.to_timedelta(df['Duration'], 's')
    will_expire = df[df['End'] <= next_month]
    filtered_list = will_expire[select_column].values.tolist()
    return filtered_list, will_expire[select_column].columns.to_list(), df


def to_excel(df, filename, condition):
    writer = pd.ExcelWriter(filename)
    row_style = lambda row: pd.Series('background-color: {}'.format('yellow' if condition(row) else 'green'), row.index)
    df.style.apply(row_style, axis=1).to_excel(writer, )
    writer.save()
    writer.close()


def makeMessage():
    # df
    next_month = np.datetime64(datetime.utcnow() + relativedelta.relativedelta(days=31))

    # ec2
    ec2_client = boto3.client('ec2', region_name=TARGET_REGION)
    ec2_response = ec2_client.describe_reserved_instances(Filters=[{'Name': 'state', 'Values': ['active']}])
    if len(ec2_response['ReservedInstances']) > 0:
        print("Exist EC2 Reserved Instance")

    ec2_ri_list = list(map(lambda a: a.values(), ec2_response['ReservedInstances']))
    ec2_head = ec2_response['ReservedInstances'][0].keys()
    ec2_df = pd.DataFrame(ec2_ri_list, columns=ec2_head)
    ec2_df['Start'] = pd.to_datetime(ec2_df['Start']).dt.tz_convert(None)
    ec2_df['End'] = pd.to_datetime(ec2_df['End']).dt.tz_convert(None)
    ec2_will_expire = ec2_df[ec2_df['End'] <= next_month]
    select_column = ['ReservedInstancesId', 'Start', 'State', 'End', 'InstanceType', 'InstanceCount']
    ec2_head2 = ec2_will_expire[select_column].columns.to_list()
    ec2_filtered_list = ec2_will_expire[select_column].values.tolist()

    # rds
    rds_client = boto3.client('rds', region_name=TARGET_REGION)
    rds_filtered_list, rds_head, rds_df = getExpRIList(rds_client.describe_reserved_db_instances(),
                                               'ReservedDBInstances',
                                               'State',
                                               'active',
                                               ['ReservedDBInstanceId', 'StartTime', 'State', 'End', 'DBInstanceClass',
                                                'DBInstanceCount'])

    # redshift
    red_client = boto3.client('redshift', region_name=TARGET_REGION)
    red_filtered_list, red_head, red_df = getExpRIList(red_client.describe_reserved_nodes(),
                                               'ReservedNodes',
                                               'State',
                                               'active',
                                               ['ReservedNodeId', 'StartTime', 'State', 'End', 'NodeType', 'NodeCount'])

    # elasticache
    ec_client = boto3.client('elasticache', region_name=TARGET_REGION)
    ec_filtered_list, ec_head, ec_df = getExpRIList(ec_client.describe_reserved_cache_nodes(),
                                             'ReservedCacheNodes',
                                             'State',
                                             'active',
                                             ['ReservedCacheNodeId', 'StartTime', 'State', 'End', 'CacheNodeType',
                                              'CacheNodeCount'])

    # elasticsearch
    es_client = boto3.client('es', region_name=TARGET_REGION)
    es_filtered_list, es_head, es_df = getExpRIList(es_client.describe_reserved_elasticsearch_instances(),
                                             'ReservedElasticsearchInstances',
                                             'State',
                                             'active',
                                             ['ReservationName', 'ReservedElasticsearchInstanceId', 'StartTime',
                                              'State', 'End', 'ElasticsearchInstanceType',
                                              'ElasticsearchInstanceCount'])

    to_excel(ec2_df, '/tmp/ec2_df.xlsx', lambda df: df['End'] <= next_month)
    to_excel(rds_df, '/tmp/rds_df.xlsx', lambda df: df['End'] <= next_month)
    to_excel(red_df, '/tmp/red_df.xlsx', lambda df: df['End'] <= next_month)
    to_excel(ec_df, '/tmp/ec_df.xlsx', lambda df: df['End'] <= next_month)
    to_excel(es_df, '/tmp/es_df.xlsx', lambda df: df['End'] <= next_month)

    html = "<!DOCTYPE html>\
          <html lang='en'>\
          <head>\
              <title>RI Status</title>\
              <style>\
                  table, th, td {\
                      border: 1px solid black;\
                      border-collapse: collapse;\
                      font-size: 10pt;\
                      width: 1500px;\
                  }\
                  th, td {\
                      padding: 5px;\
                      text-align: left;\
                  }\
              </style>\
          </head>\
          <body>" +\
           getHTMLTable("EC2", ec2_head2, ec2_filtered_list) + \
           getHTMLTable("RDS", rds_head, rds_filtered_list) + \
           getHTMLTable("Redshift", red_head, red_filtered_list) + \
           getHTMLTable("ElastiCache", ec_head, ec_filtered_list) + \
           getHTMLTable("ElasticSearch", es_head, es_filtered_list) + \
           "</body></html>"
    return html


def getHTMLTable(table_name, header, rows):
    html_middle = "<h3>{}</h3><table>{}</table>"
    table_head, table = "", ""
    table_items = []
    end_column = -1
    for index, head in enumerate(header):
        if head == "End":
            table_head += "<th bgcolor='#D45B5B'>{}</th>".format(head)
            end_column = index
        else:
            table_head += "<th>{}</th>".format(head)
    table_items.append(table_head)
    for row in rows:
        table_row = ""
        for index, item in enumerate(row):
            if index == end_column:
                table_row += "<td bgcolor='#D45B5B'>{}</td>".format(item)
            else:
                table_row += "<td>{}</td>".format(item)
        table_items.append(table_row)
    for row in table_items:
        table += "<tr>{}</tr>".format(row)
    table = html_middle.format(table_name, table)
    return table


def save_msg_to_s3(msg, bucket, obj):
    s3 = boto3.client('s3')
    s3.put_object(Body=msg, Bucket=bucket, Key=obj+"ri_exp.html")


def lambda_handler(event, context):
    # TODO implement
    dn_client = boto3.client('dynamodb', region_name=TARGET_REGION)
    dn_response = dn_client.scan(TableName='ri_exp_mailing')
    email_list = list(map(lambda a: a['email']['S'], dn_response['Items']))

    # if there is an email address validation in SES, send a validation email.
    ses_client = boto3.client('ses', region_name=SES_REGION)
    ses_list = list(filter(lambda a: '.com' in a, ses_client.list_identities()['Identities']))
    for i in email_list:
        if i not in ses_list:
            try:
                response = ses_client.verify_email_address(EmailAddress=i)
            except ClientError as e:
                print(e.response['Error']['Message'])
            else:
                print("Validation Email sent! Message ID:"),
                print(response['MessageId'])

    msg = makeMessage()
    save_msg_to_s3(msg, "ri-exp-contents", datetime.today().strftime("%Y/%m/"))
    attach = ['/tmp/ec2_df.xlsx', '/tmp/rds_df.xlsx', '/tmp/red_df.xlsx', '/tmp/ec_df.xlsx', '/tmp/es_df.xlsx']
    infos = list(map(lambda a: {'email': a, 'msg': msg, 'attach': attach}, email_list))

    procs = []
    for info in infos:
        p = Process(target=send_email, args=(info,))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()

    return {
        'statusCode': 200,
        'body': json.dumps("success", indent=4, sort_keys=True, default=str)
    }
