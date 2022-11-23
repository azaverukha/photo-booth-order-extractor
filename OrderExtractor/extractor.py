import argparse
import shutil
import sqlite3
import os
import json
import string
import sys
import logging
from datetime import date

logging.basicConfig(filename="extractorlog.txt", format="%(asctime)s %(message)s", level=logging.INFO)


def get_order_data(orderdate, db_location):
    conn = sqlite3.connect(db_location)
    logging.info("Database.db opened")
    cur = conn.cursor()
    logging.info(cur.connection)
    sql = """select DATETIME(o.CreatedOn) OrderDateTime
            , o.Oid OrderId 
            , oe.Number EnvelopeNumber
            , op.OrderLine
            , o.DirectoryName
            , o.EndUserEmail
            , o.EndUserName, o.EndUserPhone, o.MediaInput, o.Status, o.Tags
            , op.GlobalUniqueId FileName, op.FileName OriginalFileName, op.DateTaken OriginalFileDateTaken   
            , op.FixedComposedFileName
        from "Order" o 
        join "OrderEnvelope" oe on oe."Order" = o.Oid
        join "OrderLine" ol on ol.CandidateEnvelopeNumber = oe.Number
        join "OrderPage" op on op.OrderLine = ol.Oid
        where DATE(o.CreatedOn) = '{}' and o.Status =2""".format(orderdate)

    logging.debug(sql)
    res = cur.execute(sql)
    r = res.fetchall()
    cur.close()
    return r


def distinct(sequence):
    seen = list()
    for s in sequence:
        if not s in seen:
            seen.append(s)
    return seen


def run(ordersDay: date, orders_Folder: string, destination: string):
    db_location = orders_Folder + "\Database.db"
    logging.debug("DB location = ", db_location)
    try:
        if not os.path.exists(orders_Folder):
            os.mkdir(destination)
        destination = os.path.join(destination, ordersDay)
        if not os.path.exists(destination):
            os.mkdir(destination)

        orders_data = get_order_data(ordersDay, db_location)
        order_simple = distinct([[i[0], i[1], i[5], i[6]] for i in orders_data])

        for row in order_simple:
            orderMovedFolderName = row[0].replace(":", "") + ' ' + str(row[3]) + ' ' + str(row[2]).strip()
            logging.debug(orderMovedFolderName)
            orderMovedPath = os.path.join(destination, orderMovedFolderName).strip()
            logging.debug("orderMovedPath = ", orderMovedPath)
            if not os.path.exists(os.path.join(destination, orderMovedFolderName)):
                os.mkdir(os.path.join(destination, orderMovedFolderName))

            order_line = [i for i in orders_data if i[1] == row[1]]
            json_data = []
            for line in order_line:
                json_data.append(dict(OrderDateTime=line[0], OrderId=line[1], EnvelopeNumber=line[2], OrderLine=line[3],
                                      DirectoryName=line[4],
                                      EndUserEmail=line[5], EndUserName=line[6], EndUserPhone=line[7],
                                      MediaInput=line[8], Status=line[9], Tags=line[10],
                                      FileName=line[11], OriginalFileName=line[12], OriginalFileDateTaken=line[13],
                                      IsSent=False))

            logging.debug("json = ", json_data)
            with open(os.path.join(orderMovedPath, "order.json"), "w") as f:
                f.write(str(json.dumps(json_data)))
                f.close()

            logging.debug(order_line)
            for line in order_line:
                if line[14] is not None:
                    filename = str(line[14])
                else:
                    filename = str(line[11]) + '.jpg'
                fileToCopy = os.path.join(orders_Folder, "Orders", str(line[4]), "C", str(line[3]), filename)
                logging.debug("Filename to copy = ", fileToCopy)
                newfile = os.path.join(orderMovedPath, filename)
                logging.debug("Filed moved to = ", newfile)
                shutil.copy(fileToCopy, os.path.join(newfile))
                logging.info("Copying {0} to {1}".format(fileToCopy, newfile))
                print("Copying {0} to {1}".format(fileToCopy, newfile))
        logging.info("orders are extracted")
    except Exception as error:
        logging.error(error)
        print(error)


def init_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, help='Date of orders to extract', required=True)
    parser.add_argument('--source', type=str, help='Root for Order''s folder', required=True)
    parser.add_argument('--destination', type=str, help='Destination folder', required=True)
    return parser


if __name__ == '__main__':
    parser = init_parser()
    args = parser.parse_args()
    run(args.date, args.source, args.destination)
