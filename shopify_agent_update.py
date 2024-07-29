import os
import time
import math
import shopify
import streamlit as st
from shopify import Session, ShopifyResource
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, select, insert, func
from sqlalchemy.exc import SQLAlchemyError
from langchain.prompts.chat import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.utilities import SQLDatabase
from langchain.agents.agent import AgentExecutor
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.agent_toolkits.sql.prompt import SQL_FUNCTIONS_SUFFIX
from langchain_core.messages import AIMessage, SystemMessage
from langchain_core.prompts.chat import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    MessagesPlaceholder,
)
from langchain.agents import create_openai_tools_agent
from langchain.agents.agent import AgentExecutor

OPENAI_API_KEY = "sk-proj-Jgat6n1oZs9DI5MMatB7T3BlbkFJ5yRIHLV1ZoQmuWdwTnLG"

def get_db_connection_string(store_handle):
    return f"sqlite:///{store_handle}_shopify_data.db"

def create_tables(engine):
    metadata_obj = MetaData()

    products_table = Table(
        "shopify_products",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("variant_id", Integer, primary_key=True),
        Column("title", String, nullable=False),
        Column("price", Float, nullable=False),
        Column("colors", String, nullable=True),
        Column("size", String, nullable=True),
        Column("product_type", String, nullable=True),
        Column("image_paths", String, nullable=True),
    )

    orders_table = Table(
        "shopify_orders",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("email", String, nullable=False),
        Column("created_at", String, nullable=False),
        Column("total_price", Float, nullable=False),
        Column("line_items", String, nullable=True),
        Column("shipping_address", String, nullable=True),
        Column("billing_address", String, nullable=True),
    )

    cart_table = Table(
        "shopify_cart",
        metadata_obj,
        Column("cart_id", Integer, primary_key=True, autoincrement=True),
        Column("product_id", Integer, nullable=False),
        Column("variant_id", Integer, nullable=False),
        Column("title", String, nullable=False),
        Column("price", Float, nullable=False),
        Column("colors", String, nullable=True),
        Column("size", String, nullable=True),
        Column("user_id", String, nullable=False),
        Column("timestamp", String, nullable=False),
    )

    metadata_obj.create_all(engine)

    return products_table, orders_table, cart_table

def sanitize_product_type(product_type):
    return product_type.replace("'", "").replace("&", "")

def get_all_products(store_handle, api_version, token, retries=3):
    api_session = Session(store_handle, api_version, token)
    ShopifyResource.activate_session(api_session)
    shop_url = f"https://{store_handle}.myshopify.com/admin/api/{api_version}"
    ShopifyResource.set_site(shop_url)
    
    total_products = shopify.Product.count()
    num_pages = math.ceil(total_products / 250)
    
    get_next_page = True
    page = 1
    since_id = 0

    while get_next_page and page <= num_pages:
        for _ in range(retries):
            try:
                products = shopify.Product.find(since_id=since_id, limit=250)
                if not products:
                    get_next_page = False
                    break
                for product in products:
                    yield product
                if len(products) < 250:
                    get_next_page = False
                page += 1
                since_id = products[-1].id
                break
            except Exception as e:
                print(f"Error fetching products: {e}")
                time.sleep(2)
                continue
        else:
            print("Failed to fetch products after several retries.")
            break

def get_all_orders(store_handle, api_version, token, retries=3):
    api_session = Session(store_handle, api_version, token)
    ShopifyResource.activate_session(api_session)
    shop_url = f"https://{store_handle}.myshopify.com/admin/api/{api_version}"
    ShopifyResource.set_site(shop_url)
    
    total_orders = shopify.Order.count()
    num_pages = math.ceil(total_orders / 250)
    
    get_next_page = True
    page = 1
    since_id = 0

    while get_next_page and page <= num_pages:
        for _ in range(retries):
            try:
                orders = shopify.Order.find(since_id=since_id, limit=250)
                if not orders:
                    get_next_page = False
                    break
                for order in orders:
                    yield order
                if len(orders) < 250:
                    get_next_page = False
                page += 1
                since_id = orders[-1].id
                break
            except Exception as e:
                print(f"Error fetching orders: {e}")
                time.sleep(2)
                continue
        else:
            print("Failed to fetch orders after several retries.")
            break

def store_products_in_db(products, engine, table):
    with engine.begin() as connection:
        for product in products:
            for variant in product.variants:
                variant_color = ""
                variant_size = ""
                for option in product.options:
                    if option.name.lower() in ["color", "colour"]:
                        if option.position == 1:
                            variant_color = variant.option1
                        elif option.position == 2:
                            variant_color = variant.option2
                        elif option.position == 3:
                            variant_color = variant.option3
                    if option.name.lower() == "size":
                        if option.position == 1:
                            variant_size = variant.option1
                        elif option.position == 2:
                            variant_size = variant.option2
                        elif option.position == 3:
                            variant_size = variant.option3
                
                product_type = sanitize_product_type(product.product_type)

                data = {
                    "id": product.id,
                    "variant_id": variant.id,
                    "title": product.title,
                    "price": float(variant.price),
                    "colors": variant_color,
                    "size": variant_size,
                    "product_type": product_type,
                    "image_paths": ", ".join([image.src for image in product.images]),
                }

                stmt = insert(table).values(**data)
                try:
                    connection.execute(stmt)
                    print("Saved product:")
                    for key, value in data.items():
                        print(f"{key}: {value}")
                except SQLAlchemyError as e:
                    print(f"Error inserting product {product.id}: {e}")

def update_data_in_db(store_handle, api_version, token):
    db_connection_string = get_db_connection_string(store_handle)
    engine = create_engine(db_connection_string)
    products_table, orders_table, cart_table = create_tables(engine)

    latest_product_id = get_latest_id(engine, products_table)
    products = get_all_products(store_handle, api_version, token)
    new_products = (product for product in products if latest_product_id is None or product.id > latest_product_id)
    store_products_in_db(new_products, engine, products_table)

    latest_order_id = get_latest_id(engine, orders_table)
    orders = get_all_orders(store_handle, api_version, token)
    new_orders = (order for order in orders if latest_order_id is None or order.id > latest_order_id)
    store_orders_in_db(new_orders, engine, orders_table)

def store_orders_in_db(orders, engine, table):
    with engine.begin() as connection:
        for order in orders:
            line_items = ", ".join([f"{item.name} (Quantity: {item.quantity})" for item in order.line_items])
            shipping_address = f"{order.shipping_address.address1}, {order.shipping_address.city}, {order.shipping_address.province}, {order.shipping_address.zip}, {order.shipping_address.country}" if order.shipping_address else "N/A"
            billing_address = f"{order.billing_address.address1}, {order.billing_address.city}, {order.billing_address.province}, {order.billing_address.zip}, {order.billing_address.country}" if order.billing_address else "N/A"

            data = {
                "id": order.id,
                "email": order.email,
                "created_at": order.created_at,
                "total_price": float(order.total_price),
                "line_items": line_items,
                "shipping_address": shipping_address,
                "billing_address": billing_address,
            }
            
            stmt = insert(table).values(**data)
            try:
                connection.execute(stmt)
            except SQLAlchemyError as e:
                print(f"Error inserting order {order.id}: {e}")

def store_cart_in_db(cart_items, engine, table):
    with engine.begin() as connection:
        for item in cart_items:
            data = {
                "product_id": item["product_id"],
                "variant_id": item["variant_id"],
                "title": item["title"],
                "price": item["price"],
                "colors": item["colors"],
                "size": item["size"],
                "user_id": item["user_id"],
                "timestamp": item["timestamp"],
            }
            
            stmt = insert(table).values(**data)
            try:
                connection.execute(stmt)
            except SQLAlchemyError as e:
                print(f"Error inserting cart item {item['product_id']}: {e}")

def get_latest_id(engine, table):
    with engine.connect() as connection:
        latest_id = connection.execute(select(func.max(table.c.id))).scalar()
        return latest_id

SHOP_HANDLE = 'Lemmon-24april'
API_VERSION = '2024-04'
TOKEN = "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"
update_data_in_db(SHOP_HANDLE, API_VERSION, TOKEN)

db = SQLDatabase.from_uri(f"sqlite:///{SHOP_HANDLE}_shopify_data.db")
llm = ChatOpenAI(model="gpt-4o", temperature=0, verbose=True, openai_api_key=OPENAI_API_KEY)
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
context = toolkit.get_context()
tools = toolkit.get_tools()
messages = [
    HumanMessagePromptTemplate.from_template("""
        You are a proficient AI assistant specialized in querying SQL Databases to address user inquiries on Products or Orders and suggest 5 products in product recommendations.
        In Product suggestion, Display only one varients of each products.
        If the user enter the query search it on both title or product_type and where you find the product display it on screen.
        If the user asks about items on sale, provide a brief overview of the store's offerings with a heading and a selection of 5 to 6 sample products in a similar style.
        If the user ask about best selling product or realted,provide a brief overview of best selling products and selection of 5 to 6 products.
        If the user ask about "what do you sell" or related question,response with description
        For queries regarding a specific color or type of item (e.g., "blue dress","red shoes") or related keywords, filter the products accordingly and present a lineup of relevant items.
        If the user enter keywords like (e.g "womens vest","mens vest","mens t-shirt","womens t-shirt","t-shirts","mens","womens","womens dresses","womens tops","mens pants",etc) search the complete keyword in both title and Product_type and where find the product display it on screen.
        If the user enter keywords like (e.g "nike shoes","nike cape","nike shirts","nike accessories"e.t.c) search the first word keyword  in title and second word of keyword in product_type a display the products  on screen.
        If the user enter keywords(e.g "toys","shoes","accessories","mens pants") consider the complete word in both Capital and small and search it by both keyword and if finds product dispaly it on screen.Display only one varients of each products not all varients.
        If the user enter a keywords like("ladies","lady") or related consider that keywords as "womens" and if the user enter keyword like(e.g "gents") or related consider it as "mens" search it the complete keyword in both title and Product_type and where find the product display it on screen.
        If the user enter keywords like(e.g "t shirts","tshirts")or related, consider it as "t-shirts" search it the complete keyword in both title and Product_type and where find the product display it on screen.
        If the user enter abusive language keywords response it with a description.
        Don't expose the information related to database in product suggestion in any case.
        
        Important Note:
                                                    
        1- In Product suggestion, Display only one varients of each products.
        3- Product size starts with a capital letter followed by lowercase.
        4- Sizes should be in the format "Small","Medium","Large","XX-Large","X-Large","S", "M", "L", "XL", "2XL", "3XL", "4XL", "5XL", "6XL", "7XL", "8XL", "9XL", "10XL". If the user inputs in small format, ensure it is displayed in uppercase.
        5- Sizes should be in the format "Small","Medium","Large","XX-Large","X-Large". If the user inputs in small format, ensure it is displayed in uppercase.
        6- In Product suggestion, Display only one varients of each products
        7- In product suggestion always show 5 to 6 products.Display only one varients of each products
        8- If the user enter a keyword like (e.g"database","db","product table","table","cart table","order table","columns",etc) or related , dont show him any information about it ,tell him it is confidential information we cannot disclose it.
        9- If the user enter a keyword like("sure","no",etc) or related which does not have any meaning answer him with a description
        10- If the user ask about "what do you sell" or related question,response with description. 
        11- If the user enter keyword like(e.g"red ","black","gold","silver","blue","gray") or related consider it  as color. 
        12- If the user enter a word "tee shirt" consider it as "tee shirt".
        13- If the user enter keywords like(e.g "t shirts","tshirts","T-shirt","Tshirts","T-Shirts")or related, consider it as "t-shirts" search it the complete keyword in both title and Product_type and where find the product display it on screen.
        14- In Product suggestion, Display only one varients of each products.
        15- If the user enter the query search it on both title or product_type and where you find the product display it on screen.
        16- If the user enter abusive language keywords response it with a description. 
        17- Product type should be in format "Mens Pants"etc or related.If the user inputs in small format, ensure it is displayed in uppercase 
        18- If the user enter a keyword or word that does not make any sense response it with a description.
        19- Don't expose the information related to database in product suggestion in any case.
        Responses must follow the specified structure. Also, maintain the case sensitivity for uppercase and lowercase letters. Tailor the results based on the user's query to ensure relevance, especially in Women's Clothing or other categories where unrelated items like toys might be shown.
        Please present the response in the specified format:
        ### Product Suggestion
        - **Product ID:** <product_id>
        - **Variant ID:** <variant_id>
        - **Title:** <title>
        - **Price:** $<price>
        - **Colors:** <colors>
        - **Size:** <size>
        - **Product Type:** <product_type> 
        - **Image:** ![Product Image](<image_url>)                                   
        ### Order Detail
        - **Order ID:** <order_id>
        - **Email:** <email>
        - **Created At:** <created_at>
        - **Total Price:** $<total_price>
        - **Line Items:** <line_items>
        - **Shipping Address:** <shipping_address>
        - **Billing Address:** <billing_address>    
        Input Question: {input}
    """),
    AIMessage(content=SQL_FUNCTIONS_SUFFIX),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
]

prompt = ChatPromptTemplate.from_messages(messages)
prompt = prompt.partial(**context)

agent = create_openai_tools_agent(llm, tools, prompt)

agent_executor = AgentExecutor(
    agent=agent,
    tools=toolkit.get_tools(),
    verbose=True,
)

st.title("Shopify Product")
st.write("Hello! How may I be of assistance?")

query = st.text_input("Enter your query:")

db_keywords = ["database", "db", "product table", "table", "cart table", "order table", "columns","prompt","scheme","schemas","schema"]

if st.button("Submit"):
    try:
        if query.lower() in ["hello", "hi", "hey", "hy", "good morning"]:
            st.markdown("Hello! How may I help you?")
        elif any(keyword in query.lower() for keyword in db_keywords):
            st.markdown("I'm sorry, but I cannot assist with queries related to the database structure or its content. This information is confidential and cannot be disclosed.")
        else:
            response = agent_executor.invoke({"input": query})
            st.markdown(response["output"])
    except Exception as e:
        st.error(f"An error occurred: {e}")
