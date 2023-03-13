from datetime import datetime
import os
import pandas as pd
import urllib.request
from tika import parser
import json
from elasticsearch import Elasticsearch
import sys
sys.path.append('../')
from regular_expressions import reg_search
from ministry_matching import classify_document
from scraper.models import Document, PartyMember, MemberDoc
from data_analysis import produce_analysis_images
from cooccurrence_network import cooccurrence_network
# In[3]:

def download_and_save(i):
    """
    This function downloads and saves a document from the Bundestag-API with Drucksachennummer (ID)
    i and extracts features like date, predicted ministry etc.
    Parameters
    ---------
    i: index of the document to be downloaded
    Returns
    ---------
    [temp_df]: list of a dataframe
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    i=str(i)
    # padding i with 0s in the beginning for the crawler
    i= i.zfill(5)
    try:
        # download pdf
        response = urllib.request.urlopen("https://dip21.bundestag.de/dip21/btd/19/"+ i[0:3]+"/19"+i+".pdf")
        link="https://dip21.bundestag.de/dip21/btd/19/"+ i[0:3]+"/19"+i+".pdf"
        if response.status == 200:
            #save it
            file = open(os.path.join(dir_path, "helper_files/19.pdf"), 'wb')
            file.write(response.read())
            file.close()
            subject = "nan"
            title = "nan"
            whole = parser.from_file(os.path.join(dir_path, "helper_files/19.pdf"))['content']
            temp_df = {"text":whole}
            # feature extract through regex
            data = reg_search(whole)
            temp_df.update(data)
            # get ministry
            predicted_class = classify_document(whole)
            temp_df.update({'document_class': predicted_class,
                            'link': link})
        response.close()

    except Exception as e:
        print(e)
        print(str(i) + " did not work")
        return 'nan'

    return [temp_df]

def crawl():
    """
    This functions crawls the Bundestag API and populates the databases
    Return
    -------
    data_dict: dictionary of multiple documents and their features.
    """
    df = pd.DataFrame()
    print(os.getcwd())
    dir_path = os.path.dirname(os.path.realpath(__file__))
    f = open(os.path.join(dir_path, 'helper_files/last_download_index.txt'), "r+")
    last_download_index = f.read()
    f.close()
    print(last_download_index)
    temp_df=pd.DataFrame()
    last_download_index=int(last_download_index)
    i = last_download_index + 1 # get potential new index
    counter=0
    # iterating over all document IDs (Drucksachennummer) by increasing i until there are 6 IDs in a row which
    # do not have a corresponding document, signaling that the last ID has been reached (sometimes, single IDs do not have documents, but the crawling cannot stop there, so 6 docs in a row must have no document in order for the crawling to stop).
    while counter<6:
        print("Drucksachennummer:"+str(i))
        print("counter:" + str(counter))
        temp_df = download_and_save(i)
        if temp_df=='nan':
            counter+=1
            if counter==1:
                last_download_index = i - 1
        else:
            df = df.append(temp_df)
            counter+=-1
            counter=max(counter,0)
            
            # insert into DATABASES
            ingest_doc_to_ES(i, temp_df[0])
            ingest_doc_to_postgres(i, temp_df[0])
            
            if i % 20 == 0:
                with open(os.path.join(dir_path, 'helper_files/last_download_index.txt'), 'w') as a:
                    a.write(str(last_download_index))
        i+=1
    with open(os.path.join(dir_path, 'helper_files/last_download_index.txt'), 'w') as a:
        a.write(str(last_download_index))

    data_dict=df.to_dict('records')

    import json
    with open(os.path.join(dir_path, "allkleineAnfragen_in_snippets/newkleineAnfragen.json"), "w") as outfile:
        json.dump(data_dict, outfile)

    return data_dict


def ingest_doc_to_ES(doc_id, doc):
    """
    Ingesting the document including meta-data to Elastic search
    Parameters
    ----------
    doc_id: unique identifier to be used as primary key
    doc: dictionary with the text and features of the document
    """
    es = Elasticsearch()
    
    try:
        abgeordnete_lower = [i.lower().replace(" ","_") for i in doc["abgeordnete"]]
        doc["abgeordnete_lower"] = abgeordnete_lower
        es.index(index="kleine_anfragen_v3.0", doc_type="_doc", id=doc_id, body=doc)
    except Exception as e:
        failed_doc = {"document": doc,
                     "error": e}

        dt_string = datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")
        if not os.path.isdir('failed_docs_es'):
            os.mkdir('failed_docs_es')
        with open(f'failed_docs_es/{dt_string}.json', 'w') as file:
            json.dump(doc, file)

def ingest_doc_to_postgres(doc_id, doc):
    """
    ingesting the documents to postgres (including meta-data)
    Parameters
    ----------
    doc_id: unique identifier to be used as primary key
    doc: dictionary with the text and features of the document
    """
    try:
        document = Document()
        document.doc_id = doc_id
        document.type = doc['type']
        document.text = doc['text']
        document.title = doc['title']
        document.document_class = doc['document_class']
        document.topic_class = 0
        document.link = doc['link']
        document.save()
    except Exception as doc_ex:
        print('could not save document: ', doc_ex)
    abgeordnete = doc['abgeordnete']
    fraktion = doc['fraktion']

    for name in abgeordnete:
        try:
            party = PartyMember()
            if len(name) > 100:
                name = ''
            party.party = fraktion
            party.name = name
            party.save()
        except Exception as party_ex:
            print('could not save party member: ', party_ex)
        # create member document relation
        try:
            party_mem = PartyMember.objects.get(name=name, party=fraktion)
            member_doc_map = MemberDoc()
            member_doc_map.member_id = party_mem.id
            member_doc_map.doc_id = document.doc_id
            member_doc_map.save()
        except Exception as mapping_ex:
            print('could not save mapping! Exception: ', mapping_ex)


if __name__ == '__main__':
    crawl()
