from neo4j import GraphDatabase
from dotenv import load_dotenv
from flask import Flask, request, jsonify
import os

app = Flask(__name__)

# -----------------------------------------
# 1. Configure Neo4j driver
# -----------------------------------------
# You can store these in Heroku config vars
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# -----------------------------------------
# 2. Helper methods to upsert nodes & relationships
# -----------------------------------------

def upsert_account(tx, account_id, fields):
    """
    Upsert an Account node using MERGE so we don't duplicate on repeated calls.
    fields is a dict containing other attributes from Salesforce: Name, etc.
    """
    # Example: MERGE the node by external Id (Salesforce Id)
    tx.run(
        """
        MERGE (a:Account {id: $account_id})
        ON CREATE SET a += $fields
        ON MATCH SET a += $fields
        """,
        account_id=account_id,
        fields=fields
    )

def upsert_contact(tx, contact_id, fields):
    tx.run(
        """
        MERGE (c:Contact {id: $contact_id})
        ON CREATE SET c += $fields
        ON MATCH SET c += $fields
        """,
        contact_id=contact_id,
        fields=fields
    )

def upsert_product(tx, product_id, fields):
    tx.run(
        """
        MERGE (p:Product {id: $product_id})
        ON CREATE SET p += $fields
        ON MATCH SET p += $fields
        """,
        product_id=product_id,
        fields=fields
    )

def upsert_case(tx, case_id, fields):
    tx.run(
        """
        MERGE (cs:Case {id: $case_id})
        ON CREATE SET cs += $fields
        ON MATCH SET cs += $fields
        """,
        case_id=case_id,
        fields=fields
    )

def upsert_feeditem(tx, feeditem_id, fields):
    tx.run(
        """
        MERGE (f:FeedItem {id: $feeditem_id})
        ON CREATE SET f += $fields
        ON MATCH SET f += $fields
        """,
        feeditem_id=feeditem_id,
        fields=fields
    )

def upsert_note(tx, note_id, fields):
    tx.run(
        """
        MERGE (n:Note {id: $note_id})
        ON CREATE SET n += $fields
        ON MATCH SET n += $fields
        """,
        note_id=note_id,
        fields=fields
    )

def create_relationship(tx, from_label, from_id, rel_type, to_label, to_id):
    """
    Creates a relationship of type `rel_type` from one node to another.
    from_label: e.g. "Account"
    from_id: the MERGE property
    rel_type: e.g. "HAS_CONTACT"
    to_label: e.g. "Contact"
    to_id: the MERGE property
    """
    query = f"""
        MATCH (a:{from_label} {{id: $from_id}})
        MATCH (b:{to_label} {{id: $to_id}})
        MERGE (a)-[r:{rel_type}]->(b)
        RETURN r
    """
    tx.run(query, from_id=from_id, to_id=to_id)

# -----------------------------------------
# 3. Flask route to receive Salesforce data
# -----------------------------------------

@app.route('/salesforce-hook', methods=['POST'])
def salesforce_hook():
    """
    Receives a POST from Salesforce whenever a record is created/updated.
    The payload should include what object changed and relevant fields.
    """
    data = request.json  # parse JSON body

    # Example structure: 
    # {
    #   "operation": "upsert",
    #   "object": "Account",
    #   "record": {
    #       "Id": "001xx000003NG5iAAG",
    #       "Name": "Acme Inc."
    #   }
    # }
    #
    # Or for case with multiple parent references:
    # {
    #   "operation": "upsert",
    #   "object": "Case",
    #   "record": {
    #       "Id": "500xx000003xeGtAAI",
    #       "CaseNumber": "00001001",
    #       "AccountId": "001xx000003NG5iAAG",
    #       "ContactId": "003xx000004TgrHAAS",
    #       "ProductId": "01txx0000042HiwAAE"
    #   }
    # }
    #
    # You will adapt the logic here depending on your data structure.
    
    operation = data.get('operation')
    sobject = data.get('object')
    record = data.get('record', {})
    
    if not operation or not sobject or not record:
        return jsonify({"status": "error", "message": "Invalid data payload"}), 400
    
    with driver.session() as session:
        if sobject == "Account":
            account_id = record["Id"]
            fields = {k: v for k, v in record.items() if k != "Id"}
            session.execute_write(upsert_account, account_id, fields)

        elif sobject == "Contact":
            contact_id = record["Id"]
            fields = {k: v for k, v in record.items() if k != "Id"}
            session.execute_write(upsert_contact, contact_id, fields)
            
            # If there's a relationship to an Account
            if "AccountId" in record and record["AccountId"]:
                session.execute_write(
                    create_relationship,
                    "Account", record["AccountId"],
                    "HAS_CONTACT",
                    "Contact", contact_id
                )

        elif sobject == "Product":
            product_id = record["Id"]
            fields = {k: v for k, v in record.items() if k != "Id"}
            session.execute_write(upsert_product, product_id, fields)

        elif sobject == "Case":
            case_id = record["Id"]
            fields = {k: v for k, v in record.items() if k != "Id"}
            session.execute_write(upsert_case, case_id, fields)
            
            # If there's a relationship to Account
            if "AccountId" in record and record["AccountId"]:
                session.execute_write(
                    create_relationship,
                    "Account", record["AccountId"],
                    "HAS_CASE",
                    "Case", case_id
                )
            
            # If there's a relationship to Contact
            if "ContactId" in record and record["ContactId"]:
                session.execute_write(
                    create_relationship,
                    "Contact", record["ContactId"],
                    "HAS_CASE",
                    "Case", case_id
                )
            
            # If there's a relationship to Product
            if "ProductId" in record and record["ProductId"]:
                session.execute_write(
                    create_relationship,
                    "Product", record["ProductId"],
                    "HAS_CASE",
                    "Case", case_id
                )

        elif sobject == "FeedItem":
            feeditem_id = record["Id"]
            fields = {k: v for k, v in record.items() if k != "Id"}
            session.execute_write(upsert_feeditem, feeditem_id, fields)
            
            # If there's a relationship to a Case
            if "ParentId" in record and record["ParentId"]:
                # Suppose FeedItem's ParentId references the Case Id
                session.execute_write(
                    create_relationship,
                    "Case", record["ParentId"],
                    "HAS_FEEDITEM",
                    "FeedItem", feeditem_id
                )

        elif sobject == "Note":
            note_id = record["Id"]
            fields = {k: v for k, v in record.items() if k != "Id"}
            session.execute_write(upsert_note, note_id, fields)
            
            # If there's a relationship to a Case
            if "ParentId" in record and record["ParentId"]:
                session.execute_write(
                    create_relationship,
                    "Case", record["ParentId"],
                    "HAS_NOTE",
                    "Note", note_id
                )

        else:
            # Could return an error or simply note we haven't handled that SObject
            return jsonify({"status": "error", "message": f"Unhandled sobject: {sobject}"}), 400

    return jsonify({"status": "success"}), 200


@app.route('/', methods=['GET'])
def health_check():
    return "Salesforce to Neo4j integration is running.", 200

# For local testing
if __name__ == "__main__":
    app.run(debug=True, port=5000)