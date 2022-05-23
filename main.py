# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import sqlite3
import sys

#DTO
class Hat:
    def __init__(self,id,topping,supplier,quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity

class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Order:
    def __init__(self, id, location,hat):
        self.id = id
        self.location = location
        self.hat = hat


#DAO
class _Hats:
    def __init__(self,connection):
        self._connection = connection

    def insert_hat(self,hatDB):
        self._connection.execute("""
        INSERT INTO HATS (id,topping,supplier,quantity)
         VALUES (?,?,?,?)""",[hatDB.id,hatDB.topping,hatDB.supplier,hatDB.quantity])
    def find(self,hatID):
        cur = self._connection.crusor()
        cur.excute("""
        SELECT * from HATS WHERE id = ?
        """,[hatID])
        return Hat(*cur.fetchone())


class _Suppliers:

    def __init__(self,connection):
        self._connection = connection
    def insert_supplier(self,supplierDTO):
        self._connection.execute("""
        INSERT INTO SUPPLIERS (id,name)
         VALUES (?,?)""",[supplierDTO.id,supplierDTO.name])

    def find(self,supplier_id):
        cur = self._connection.crusor()
        cur.excute("""
               SELECT * from SUPPLIERS WHERE id = ?
               """, [supplier_id])
        return Supplier(*cur.fetchone())


class _Orders:

    def __init__(self, connection):
        self._connection = connection

    def insert_order(self, orderDTO):
        self._connection.execute("""
        INSERT INTO ORDERS (id,location,hat)
         VALUES (?,?,?)""", [orderDTO.id, orderDTO.location,orderDTO.hat])
        self._connection.commit()

    def find(self, order_id):
        cur = self._connection.crusor()
        cur.execute("""
               SELECT * from ORDERS WHERE id = ?
               """, [order_id])
        cur.excute()
        return Order(*cur.fetchone())



#Repository
class _Repository:
    def __init__(self):
        self._connection = sqlite3.connect(sys.argv[4])
        self.HatsDAO = _Hats(self._connection)
        self.supplierDAO = _Suppliers(self._connection)
        self.orderDAO = _Orders(self._connection)
        self.outputFile = ""

    def createHatsTable(self):
        self._connection.execute("""
           CREATE TABLE HATS (
               id INTEGER PRIMARY KEY,
               topping STRING NOT NULL,
               supplier INTEGER REFERENCES supplier(id),
               quantity INTEGER NOT NULL)
           """)
        return

    def createSuppliersTable(self):
        self._connection.execute("""
         CREATE TABLE SUPPLIERS (
            id INTEGER PRIMARY KEY,
            name STRING NOT NULL)
            """)
        return

    def createOrdersTable(self):
        self._connection.execute("""
        CREATE TABLE ORDERS (
        id INTEGER PRIMARY KEY,
        location STRING NOT NULL,
        hat INTEGER REFERENCES hats(id))
        """)
        return

    def DeleteHats(self):
        cursor = self._connection.cursor()
        cursor.execute("DELETE FROM HATS WHERE quantity = 0")
        self._connection.commit()


    def excuteOrder(self,order,orderID):
        location , topping = order[0],order[1]
        cur = self._connection.cursor()
        cur.execute("""
        SELECT HATS.id, HATS.supplier FROM HATS
        WHERE HATS.quantity > 0 AND HATS.topping = (?)
        ORDER BY HATS.supplier  """, [topping])
        try:
            orderHatId , supplierID = cur.fetchone()
        except:
            return
        curOrder =  Order(orderID,location,orderHatId)
        self.orderDAO.insert_order(curOrder)

        #update
        cur.execute("UPDATE HATS SET quantity = quantity-1 WHERE id = (?)",[orderHatId])
        self._connection.commit()

        #updateOutput
        cur.execute("SELECT name FROM SUPPLIERS WHERE id = (?)",[supplierID])
        supplierName = cur.fetchone()[0]
        curOutput = ",".join((topping,supplierName,location))
        self.outputFile += curOutput+"\n"



def main(args):
    if (len(args) != 5):
        print("Missing argument")

    repository = _Repository()
    configFile = args[1]
    ordersFile = args[2]
    outputFile = args[3]
    clean = open(outputFile, 'w')
    clean.close()
    databaseFile = args[4]
    clean = open(databaseFile, 'w')
    clean.close()
    repository.createHatsTable()
    repository.createSuppliersTable()
    repository.createOrdersTable()

    # Parse configFile
    with open(configFile) as F:
        allLines = F.readlines()

    numsLine = allLines[0].split(',')
    numOfHats = int(numsLine[0])
    j=0
    allLines = allLines[1:]
    for line in allLines:
        line = line.rstrip()
        currLine = line.split(',')
        if (j < numOfHats):
            currHat = Hat(*currLine)
            repository.HatsDAO.insert_hat(currHat)
        else:
            currSupplier = Supplier(*currLine)
            repository.supplierDAO.insert_supplier(currSupplier)
        j+=1

    # Parse ordersFile
    with open(ordersFile) as O:
        allLines = O.readlines()
    orderId = 1
    for line in allLines:
        line = line.rstrip()
        currOrder = line.split(',')
        repository.excuteOrder(currOrder, orderId)
        orderId += 1
    repository.DeleteHats()

    # Create output file

    if(outputFile != None):
        O = open(outputFile, "w")
        O.write(str(repository.outputFile))

    repository._connection.commit()
    repository._connection.close()


if __name__ == '__main__':
    main(sys.argv)