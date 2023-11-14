from pymongo import MongoClient
import certifi
from bson import ObjectId
import json
from bs4 import BeautifulSoup, NavigableString
from selenium import webdriver
import base64
from pymongo import MongoClient
import certifi
import boto3
import io
from botocore.exceptions import NoCredentialsError
from pymongo import MongoClient
from pymongo.errors import PyMongoError

def file_upload(pdf_data, irn):
    pdf_bytes = io.BytesIO(pdf_data)
    aws_access_key = "AKIAWVKQBF2LSY6UFLHK"
    aws_secret_key = "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"
    aws_region = "ap-south-1"
    bucket_name = 'qr-ocr-data'
    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    s3 = session.client('s3')
    folder = "Invoices"
    filepath = f"{folder}/{irn}.pdf"
    try:
        s3.upload_fileobj(pdf_bytes, bucket_name, filepath, ExtraArgs=None, Callback=None, Config=None)
        # print(f'Uploaded file to S3 as {irn}.pdf')
        document_url = f"https://{bucket_name}.s3.{aws_region}.amazonaws.com/{filepath}"
        return document_url
    except Exception as e:
        print(f'Error uploading {filepath} to S3: {str(e)}')

state_code_to_name = {
    '01': 'Jammu And Kashmir',
    '02': 'Himachal Pradesh',
    '03': 'Punjab',
    '04': 'Chandigarh',
    '05': 'Uttarakhand',
    '06': 'Haryana',
    '07': 'Delhi',
    '08': 'Rajasthan',
    '09': 'Uttar Pradesh',
    '10': 'Bihar',
    '11': 'Sikkim',
    '12': 'Arunachal Pradesh',
    '13': 'Nagaland',
    '14': 'Manipur',
    '15': 'Mizoram',
    '16': 'Tripura',
    '17': 'Meghlaya',
    '18': 'Assam',
    '19': 'West Bengal',
    '20': 'Jharkhand',
    '21': 'Odisha',
    '22': 'Chattisgarh',
    '23': 'Madhya Pradesh',
    '24': 'Gujarat',
    '26': 'Dadra And Nagar Haveli And Daman And Diu',
    '27': 'Maharashtra',
    '28': 'Andhra Pradesh',
    '29': 'Karnataka',
    '30': 'Goa',
    '31': 'Lakshwadeep',
    '32': 'Kerala',
    '33': 'Tamil Nadu',
    '34': 'Puducherry',
    '35': 'Andaman And Nicobar Islands',
    '36': 'Telangana',
    '37': 'Andhra Pradesh',
    '38': 'Ladakh',
}

def get_state_name(state_code):
    state_code_str = str(state_code).zfill(2)
    return state_code_to_name.get(state_code_str, 'Unknown')
class MongoDBManager:
    def __init__(self, url):
        self.client = None
        self.url = url

    def __enter__(self):
        self.client = MongoClient(self.url, tls=True, tlsCAFile=certifi.where())
        self.client.admin.command('ismaster')
        print("MongoDB connection is successful.")
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb): 
        if self.client:
            self.client.close()
            print("MongoDB connection is closed.")

url = "mongodb://doadmin:8eqby75zk2Ng1634@db-mongodb-blr1-91426-e07cde9a.mongo.ondigitalocean.com:27017/?authSource=admin&tls=true"

with MongoDBManager(url) as client:
    db = client['gst_portal']
    collection = db['einvoice_irn_raw']
    document = collection.find()
    # document = collection.find_one({'_id': ObjectId('65454f58d3dface59c952fe0')})
     

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options=options)

    tableContent = """<tbody>
                <td id="sno" style="width: 9%;">0001.</td>
                <td id="desc" style="width: 9%;">GEMS.SURPRISE.15.8G.RS.45.RUDRA.RANGE</td>
                <td id="hsncode" style="width: 9%;">18069010</td>
                <td id="quantity" style="width: 9%;">75</td>
                <td id="unit" style="width: 9%;">CTN</td>
                <td id="unit_price" style="width: 9%;">2430</td>
                <td id="discount" style="width: 9%;">0</td>
                <td id="taxable" style="width: 9%;">182250</td>
                <td id="tax_rate" style="width: 9%;">18+Undefined|Undefined + undefinded</td>
                <td id="others" style="width: 9%;"></td>
                <td id="total" style="width: 9%;">215055</td>
            </tbody>"""

    num_documents = collection.count_documents({"AckNo": 122316676536280})
    for i in range(0, 10):
        file_path = "7nov_invoice.html"
        with open(file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
            soup = BeautifulSoup(html_content, 'html.parser') 
        obj = {
        'header_gstin': document[i]['SignedInvoiceParsed']['SellerDtls']['Gstin'],
        'header_name': document[i]['SignedInvoiceParsed']['SellerDtls']['LglNm'],
        'irn': document[i]['SignedInvoiceParsed']['Irn'],
        'ackno': document[i]['AckNo'] if 'AckNo' in document[i] else '',
        'ackdt': document[i]['AckDt'] if 'AckDt' in document[i] else '',
        'category': document[i]['SignedInvoiceParsed']['TranDtls']['SupTyp'] if 'SupTyp' in document[i]['SignedInvoiceParsed']['TranDtls'] else '',
        'docno' : document[i]['SignedInvoiceParsed']['DocDtls']['No'] if  'No' in document[i]['SignedInvoiceParsed']['DocDtls'] else '',
        'doctype': document[i]['SignedInvoiceParsed']['DocDtls']['Typ'] if 'Typ' in document[i]['SignedInvoiceParsed']['DocDtls'] else '',
        'docdt': document[i]['SignedInvoiceParsed']['DocDtls']['Dt'] if 'Dt' in document[i]['SignedInvoiceParsed']['DocDtls'] else '',
        'seller_gstin': document[i]['SignedInvoiceParsed']['SellerDtls']['Gstin'] if 'Gstin' in document[i]['SignedInvoiceParsed']['SellerDtls'] else '',
        'seller_legal_name': document[i]['SignedInvoiceParsed']['SellerDtls']['LglNm'] if 'LglNm' in document[i]['SignedInvoiceParsed']['SellerDtls'] else '',
        'seller_trade_name': document[i]['SignedInvoiceParsed']['SellerDtls']['TrdNm'] if 'TrdNm' in document[i]['SignedInvoiceParsed']['SellerDtls'] else '',
        'seller_addr1': document[i]['SignedInvoiceParsed']['SellerDtls']['Addr1'],
        'seller_addr2': document[i]['SignedInvoiceParsed']['SellerDtls']['Addr2'] if 'Addr2' in document[i]['SignedInvoiceParsed']['SellerDtls'] else '',
        'seller_loc': document[i]['SignedInvoiceParsed']['SellerDtls']['Loc'],
        'seller_pin': document[i]['SignedInvoiceParsed']['SellerDtls']['Pin'],
        'seller_state_code': document[i]['SignedInvoiceParsed']['SellerDtls']['Stcd'],
        'buyer_gstin': document[i]['SignedInvoiceParsed']['BuyerDtls']['Gstin'],
        'buyer_legal_name': document[i]['SignedInvoiceParsed']['BuyerDtls']['LglNm'],
        'buyer_trade_name': document[i]['SignedInvoiceParsed']['BuyerDtls']['TrdNm'] if 'TrdNm' in document[i]['SignedInvoiceParsed']['BuyerDtls'] else '',
        'buyer_addr1': document[i]['SignedInvoiceParsed']['BuyerDtls']['Addr1'],
        'buyer_addr2': document[i]['SignedInvoiceParsed']['BuyerDtls']['Addr2'] if 'Addr2' in document[i]['SignedInvoiceParsed']['BuyerDtls'] else '',
        'buyer_loc': document[i]['SignedInvoiceParsed']['BuyerDtls']['Loc'],
        'buyer_pin': document[i]['SignedInvoiceParsed']['BuyerDtls']['Pin'],
        'buyer_state_code': document[i]['SignedInvoiceParsed']['BuyerDtls']['Stcd'],
        'taxable_amt' : document[i]['SignedInvoiceParsed']['ValDtls']['AssVal'],
        'cgst_amt': document[i]['SignedInvoiceParsed']['ValDtls']['CgstVal'],
        'sgst_amt': document[i]['SignedInvoiceParsed']['ValDtls']['SgstVal'],
        'igst_amt': document[i]['SignedInvoiceParsed']['ValDtls']['SgstVal'],
        'cess_amt' : document[i]['SignedInvoiceParsed']['ValDtls']['CesVal'],
        'state_cess_amt': document[i]['SignedInvoiceParsed']['ValDtls']['StCesVal'],
        'total_amt': document[i]['SignedInvoiceParsed']['ValDtls']['TotInvVal'],
        'qr_code': document[i]['SignedQRCode']
        }
        data = document[i]['SignedInvoiceParsed']['ItemList']
        ItemList = []
        serial_num =1
        for item in data:
            cgst_val = item['ValDtls']['CgstVal']
            sgst_val = item['ValDtls']['SgstVal']
            igst_val = item['ValDtls']['IgstVal']
            ces_val = item['ValDtls']['CesVal']
            st_ces_val = item['ValDtls']['StCesVal']
            ass_amt = item['AssAmt']
            gst_rt = item['GstRt']
            total_inv_val = item['ValDtls']['TotInvVal']
            
            formatted_item = [
                str(serial_num).zfill(1), 
                item['PrdDesc'],
                item['HsnCd'],
                str(item['Qty']),
                item['Unit'],
                str(item['UnitPrice']),
                str(cgst_val + sgst_val + ces_val + st_ces_val), 
                str(ass_amt),
                f"{gst_rt}+{cgst_val}|{sgst_val}+{igst_val}", 
                "",
                str(total_inv_val)
            ]
            serial_num += 1
            ItemList.append(formatted_item)
        
        table = soup.find('table',id="table_div")

        tablesToBeDeleted = soup.find('tbody', id = 'tableBody')
        if tablesToBeDeleted:
            tablesToBeDeleted.decompose()


        for index, item in enumerate(ItemList, start=0):
            row_html = f"""
            <tbody>
                <td id="sno_{index}" style="width: 9%;">{item[0]}</td>
                <td id="desc_{index}" style="width: 9%;">{item[1]}</td>
                <td id="hsncode_{index}" style="width: 9%;">{item[2]}</td>
                <td id="quantity_{index}" style="width: 9%;">{item[3]}</td>
                <td id="unit_{index}" style="width: 9%;">{item[4]}</td>
                <td id="unit_price_{index}" style="width: 9%;">{item[5]}</td>
                <td id="discount_{index}" style="width: 9%;">{item[6]}</td>
                <td id="taxable_{index}" style="width: 9%;">{item[7]}</td>
                <td id="tax_rate_{index}" style="width: 9%;">{item[8]}</td>
                <td id="others_{index}" style="width: 9%;">{item[9]}</td>
                <td id="total_{index}" style="width: 9%;">{item[10]}</td>
            </tbody>
            """
            table.append(BeautifulSoup(row_html, 'html.parser'))

        # print("ItemList =", ItemList)
        print("ItemList =", len(ItemList))
        header_gstin = soup.find('td', id='header_gstin')
        if header_gstin and header_gstin.string:
            header_gstin.string.replace_with(str(obj['header_gstin'] + " " + obj['header_name']))

        irn = soup.find('td', id='irn')
        if irn:
            parts = irn.text.split(':')
            if len(parts) > 1:
                irn.string.replace_with(parts[0] + ': ' + str(obj['irn']))

        ackno = soup.find('td', id='ackno')
        if ackno and ackno.string:
            parts = ackno.text.split(':')
            if len(parts) > 1:
                ackno.string.replace_with(parts[0] + ': ' + str(obj['ackno']))

        ackdt = soup.find('td', id='ackdt')
        if ackdt and ackdt.string:
            parts = ackdt.text.split(':')
            if len(parts) > 1:
                ackdt.string.replace_with(parts[0] + ': ' + str(obj['ackdt']))
        
        category = soup.find('td', id='category')
        if category and category.string:
            parts = category.text.split(':')
            if len(parts) > 1:
                category.string.replace_with(parts[0] + ': ' + str(obj['category']))

        docno = soup.find('td', id='docno')
        if docno and docno.string:
            parts = docno.text.split(':')
            if len(parts) > 1:
                docno.string.replace_with(parts[0] + ': ' + str(obj['docno']))

        doctype = soup.find('td', id='doctype')
        if doctype and doctype.string:
            parts = doctype.text.split(':')
            if len(parts) > 1:
                doctype.string.replace_with(parts[0] + ': ' + str(obj['doctype']))
        
        docdt = soup.find('td', id='docdt')
        if docdt and docdt.string:
            parts = docdt.text.split(':')
            if len(parts) > 1:
                docdt.string.replace_with(parts[0] + ': ' + str(obj['docdt']))

        ecom_gstin = soup.find('td', id='ecom_gstin')
        if ecom_gstin and ecom_gstin.string:
            parts = ecom_gstin.text.split(':')
            if len(parts) > 1:
                ecom_gstin.string.replace_with(parts[0] + ': ' + str(obj['header_gstin']))
        
        seller_gstin = soup.find('td', id='seller_gstin')
        if seller_gstin and seller_gstin.string:
            seller_gstin.string.replace_with(str(obj['seller_gstin']))
        
        seller_legal_name = soup.find('td', id='seller_legal_name')
        if seller_legal_name and seller_legal_name.string:
            seller_legal_name.string.replace_with(str(obj['seller_legal_name']))

        seller_trade_name = soup.find('td', id='seller_trade_name')
        if seller_trade_name and seller_trade_name.string:
            seller_trade_name.string.replace_with(str(obj['seller_trade_name']))

        seller_addr1 = soup.find('td', id='seller_addr1')
        if seller_addr1 and seller_addr1.string:
            seller_addr1.string.replace_with(str(obj['seller_addr1']))
        
        seller_addr2 = soup.find('td', id='seller_addr2')
        if seller_addr2 and seller_addr2.string:
            seller_addr2.string.replace_with(str(obj['seller_addr2']))
        
        seller_loc = soup.find('td', id='seller_loc')
        if seller_loc and seller_loc.string:
                seller_loc.string.replace_with(str(obj['seller_loc']))
        
        seller_pin = soup.find('td', id='seller_pin')
        if seller_pin and seller_pin.string:
            seller_pin.string.replace_with(str(obj['seller_pin']))
        
        seller_state = soup.find('td', id='seller_state')
        seller_state_name = get_state_name(obj['seller_state_code'])
        if seller_state and seller_state.string:
            seller_state.string.replace_with(str(seller_state_name))

        buyer_gstin = soup.find('td', id='buyer_gstin')
        if buyer_gstin and buyer_gstin.string:
            buyer_gstin.string.replace_with(str(obj['buyer_gstin']))
        
        buyer_legal_name = soup.find('td', id='buyer_legal_name')
        if buyer_legal_name and buyer_legal_name.string:
            buyer_legal_name.string.replace_with(str(obj['buyer_legal_name']))

        buyer_trade_name = soup.find('td', id='buyer_trade_name')
        if buyer_trade_name and buyer_trade_name.string:
            buyer_trade_name.string.replace_with(str(obj['buyer_trade_name']))

        buyer_addr1 = soup.find('td', id='buyer_addr1')
        if buyer_addr1 and buyer_addr1.string:
            buyer_addr1.string.replace_with(str(obj['buyer_addr1']))
        
        buyer_addr2 = soup.find('td', id='buyer_addr2')
        if buyer_addr2 and buyer_addr2.string:
            buyer_addr2.string.replace_with(str(obj['buyer_addr2']))
        
        buyer_loc = soup.find('td', id='buyer_loc')
        if buyer_loc and buyer_loc.string:
            buyer_loc.string.replace_with(str(obj['buyer_loc']))
        
        buyer_pin = soup.find('td', id='buyer_pin')
        if buyer_pin and buyer_pin.string:
            buyer_pin.string.replace_with(str(obj['buyer_pin']))
        
        buyer_state = soup.find('td', id='buyer_state')
        buyer_state_name = get_state_name(obj['buyer_state_code'])
        if buyer_state and buyer_state.string:
            buyer_state.string.replace_with(str(buyer_state_name))
        
        shipto_gstin = soup.find('td', id='shipto_gstin')
        if shipto_gstin and shipto_gstin.string:
            shipto_gstin.string.replace_with(str(obj['buyer_gstin']))

        shipto_legal_name = soup.find('td', id='shipto_legal_name')
        if shipto_legal_name and shipto_legal_name.string:
            shipto_legal_name.string.replace_with(str(obj['buyer_legal_name']))

        shipto_trade_name = soup.find('td', id='shipto_trade_name')
        if shipto_trade_name and shipto_trade_name.string:
            shipto_trade_name.string.replace_with(str(obj['buyer_trade_name']))

        shipto_loc = soup.find('td', id='shipto_loc')
        if shipto_loc and shipto_loc.string:
            shipto_loc.string.replace_with(str(obj['buyer_addr1']))

        shipto_state = soup.find('td', id='shipto_state')
        shipto_state_name = get_state_name(obj['buyer_state_code'])
        if shipto_state and shipto_state.string:
            shipto_state.string.replace_with(str(shipto_state_name))
        
        place_of_supply = soup.find('td', id='place_of_supply')
        pos_state = get_state_name(obj['seller_state_code'])
        if place_of_supply and place_of_supply.string:
            parts = place_of_supply.text.split(':')
            if len(parts) > 1:
                place_of_supply.string.replace_with(parts[0] + ': ' + str(pos_state))
        
        taxable_amt = soup.find('td', id='taxable_amt')
        if taxable_amt and taxable_amt.string:
            taxable_amt.string.replace_with(str(obj['taxable_amt']))
        
        cgst_amt = soup.find('td', id='cgst_amt')
        if cgst_amt and cgst_amt.string:
            cgst_amt.string.replace_with(str(obj['cgst_amt']))

        sgst_amt = soup.find('td', id='sgst_amt')
        if sgst_amt and sgst_amt.string:
            sgst_amt.string.replace_with(str(obj['sgst_amt']))

        igst_amt = soup.find('td', id='igst_amt')
        if igst_amt and igst_amt.string:
            igst_amt.string.replace_with(str(obj['igst_amt']))

        cess_amt = soup.find('td', id='cess_amt')
        if cess_amt and cess_amt.string:
            cess_amt.string.replace_with(str(obj['cess_amt']))
        
        state_cess_amt = soup.find('td', id='state_cess_amt')
        if state_cess_amt and state_cess_amt.string:
            state_cess_amt.string.replace_with(str(obj['state_cess_amt']))
        
        total_amt = soup.find('td', id='total_amt')
        if total_amt and total_amt.string:
            total_amt.string.replace_with(str(obj['total_amt']))

    
        generated_by = soup.find('td', id='generated_by')
        if generated_by and generated_by.string:
            parts = generated_by.text.split(':')
            if len(parts) > 1:
                generated_by.string.replace_with(parts[0] + ': ' + str(obj['header_gstin']))

        signed_dt = soup.find('td', id='signed_dt')
        if signed_dt and signed_dt.string:
            parts = signed_dt.text.split(':')
            if len(parts) > 1:
                signed_dt.string.replace_with(parts[0] + ': ' + str(obj['ackdt']))

        qr_image = f"""<img width="230px" height="230px" src="https://chart.googleapis.com/chart?cht=qr&chl={obj['qr_code']}&chs=60x200&choe=UTF-8&chld=L%7C2">"""
        qr_atag = soup.find('a', id='qr_code')
        img_tag = qr_atag.find('img')
        if img_tag:
            img_tag = BeautifulSoup(str(qr_image), 'html.parser')

        new_barcode = f"""<img alt='Barcode Generator TEC-IT' width="113" height="44"
                    src='https://barcode.tec-it.com/barcode.ashx?data={obj['ackno']}&code=Code128&translate-esc=on'/>"""
        barcode_td = soup.find('td', id='bar_code')
        barcode_img = barcode_td.find('img')
        if barcode_img: 
            barcode_img.replace_with(BeautifulSoup(str(new_barcode), 'html.parser'))

        

        # rowsss = f"""<tbody>
        #             <td style="width: 9%;">0001.</td>
        #             <td style="width: 9%;">GEMS.SURPRISE.15.8G.RS.45.RUDRA.RANGE</td>
        #             <td style="width: 9%;">18069010</td>
        #             <td style="width: 9%;">75</td>
        #             <td style="width: 9%;">CTN</td>
        #             <td style="width: 9%;">2430</td>
        #             <td style="width: 9%;">0</td>
        #             <td style="width: 9%;">182250</td>
        #             <td style="width: 9%;">18+Undefined|Undefined + undefinded</td>
        #             <td style="width: 9%;"></td>
        #             <td style="width: 9%;">215055</td>
        #         </tbody>"""
        # table.append(BeautifulSoup(rowsss, 'html.parser'))
        # table.append(BeautifulSoup(rowsss, 'html.parser'))
        # table.append(BeautifulSoup(rowsss, 'html.parser'))

        
        
        html_string = soup.prettify()
        file_name = "output.html"

        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(html_string)

        encoded_html = base64.b64encode(html_string.encode('utf-8'))

        data_uri = 'data:text/html;base64,' + encoded_html.decode('utf-8')
        driver.get(data_uri)
        driver.implicitly_wait(1)

        pdf_content = driver.execute_cdp_cmd('Page.printToPDF', {
            'landscape': False,
            'printBackground': True,
            'preferCSSPageSize': True,
        })

        pdf_data = base64.b64decode(pdf_content['data'])
        doc_url = file_upload(pdf_data, obj['irn'])
        if doc_url: 
            print('doc_url', doc_url)
            try:
                result = collection.update_one(
                    {'_id': document[i]['_id']},
                    {'$set': {'url': doc_url}} 
                )
                if result.matched_count:
                    print(f"Document id: {document[i]['_id']} updated!")
                else:
                    print(f"No document found with id: {document[i]['_id']}.")
            except PyMongoError as e:
                print(f"An error occurred: {e}")
        else:
            print("No URL provided to update the document")
        
    driver.quit()