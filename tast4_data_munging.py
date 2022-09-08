import csv
import sqlite3

#establishing connection
connection = sqlite3.connect("shipment_database.db")
if not connection:
  exit("could not exstablish connection")

#get names of products from csv0
def extractProducts():
  prod_id = 0
  product_names = {}
  with open('shipping_data_csv0', newline='') as csvfile:
    row_reader = csv.DictReader(csvfile)
    for row in row_reader:
      product_names[row['product']] = prod_id
      prod_id += 1
  return product_names
    
#insert products into product table in database
def insertProductsIntoDB(products):
  insert_products_query = 'INSERT INTO product (id, name) VALUES (?, ?);'
  for product,prod_id in products.items():
    try:
      cursor = connection.cursor()
      data_tuple = (prod_id,product)
      cursor.execute(insert_products_query, data_tuple)
      connection.commit()
      cursor.close()
    except sqlite3.Error:
      print("Failed to insert {}, {} into product table".format(prod_id,product))

#get shipments from csv1 and csv2
def extractShipments1():
  shipment_details = {}
  #from csv1, we need to get the count of each product for each shipment
  with open('shipping_data_csv1', newline='') as csvfile1:
    row_reader = csv.DictReader(csvfile1)
    for row in row_reader:
      ship_id = row['shipment_identifier']
      prod = row['product']
      if not shipment_details[ship_id]: #add dict if we have not seen this shipment yet
        shipment_details[ship_id] = dict()
      if not shipment_details[ship_id][prod]:
        shipment_details[ship_id][prod] = 0
      shipment_details[ship_id][prod] += 1
  return shipment_details
  
def extractShipments2():
  shipment_details = {}
  with open('shipping_data_csv2') as csvfile2:
    row_reader = csv.reader(csvfile2)
    for row in row_reader:
      data = row.split()
      ship_id, origin, dest = data[0], data[1], data[2]
      shipment_details[ship_id] = [origin, dest]
  return shipment_details

def insertShipmentsIntoDB(product_ids, ship_dict1, ship_dict2):
  insert_shipments_query = 'INSERT INTO shipment (id,product_id,quantity,origin,destination) VALUE (?,?,?,?,?)'
  for ship_id, prod_dict in ship_dict1.item():
    for product, count in prod_dict.items():
      try:
        cursor = connection.cursor()
        prod_id, ship_origin, ship_dest = product_ids[product], ship_dict2[ship_id][0], ship_dict2[ship_id][1]
        data_tuple = (ship_id, prod_id, count, ship_origin, ship_dest)
        cursor.execute(insert_shipments_query, data_tuple)
        connection.commit()
        cursor.close()
      except sqlite3.Error:
        print("Could not add ship_id: {}, product: {} to database".format(ship_id, product))
  
#handles product table
product_dict = extractProducts()
insertProductsIntoDB(product_dict)

#handles shipment table
insertShipmentsIntoDB(product_dict, extractShipments1(), extractShipments2())