import re
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.client import IndicesClient

es = Elasticsearch()
#es.index(index="my-index", doc_type="test-type", id=42, body={"any": "data"})
#es.get(index="my-index", doc_type="test-type", id=42)['_source']

def create_index():
    #setmap = {"settings": {"analysis": {"analyzer": "dutch"}}, "mappings": {"product": {"properties": {"description": {"type": "string", "fields": {"du" : {"type": "string", "analyzer": "dutch"}}}}}}}
    setmap = {
          "settings": {
            "analysis": {
              "filter": {
                "alphanumeric" : {
                    "type": "word_delimiter",
                    "generate_word_parts": "false",
                    "generate_number_parts": "false",
                    "catenate_words": "true",
                    "catenate_numbers": "true",
                    "preserve_original": "true",
                    "split_on_numerics": "true",
                    "stem_english_possessive": "false"
                    }
                },
                  "analyzer": {
                    "tessas_analyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": ["lowercase", "alphanumeric"]
                    }
                 }
              }
          },          
          "mappings": {
            "product": {
                "properties": {
                    "description": {
                        "type": "string",
                        "analyzer" : "tessas_analyzer"
                        }
                    }
                }
            }         
        }

    IndicesClient(es).create(index="products", body=setmap)
    print("created index...")
    
def do_indexing():
    r = re.compile(r"([0-9]+)\t(.+)")
    product_array = [] 
    
    coll = open('C:/Users/TessaElfrink/Documents/Uni/IR/dataset/collection.txt', encoding="utf-8")
    #first_line = "1\tVDI-gereedschaphouder E1 50x32mm"
    lines = coll.readlines()
    
    i = 0
    for l in lines:
        if i > 0:
            line = l.rstrip()
            m = re.match(r, line)
            if m:
                #product_obj = {"_index": "products", "_type": "product", "_id": m.group(1), "body": {"id": m.group(1), "description": m.group(2)}}
                product_obj = {"_index": "products", "_type": "product", "_id": m.group(1), "_source": {"id": m.group(1), "description": m.group(2)}}
                product_array.append(product_obj)
            else:
                print("no match")
        i += 1
        
    helpers.bulk(es, product_array)  
    #es.index(index="products", doc_type="product", body=product_array[0]["body"], id=product_array[0]["_id"])
    IndicesClient(es).flush(index="products")
    print("finished indexing...")

def delete_index():
    IndicesClient(es).delete(index="products")
    print("deleted index...")
    
def run_queries():
    query_file = open(r"C:\Users\TessaElfrink\Documents\Uni\IR\dataset\500queries.txt", encoding="utf-8")
    queries = query_file.readlines()
    q_dict = {}
    for q in queries:
        q = q.rstrip()
        q_id, query = q.split("\t")
        q_dict[q_id] = query    
               
    #try for one query first, look at format       
    #qu = {"query" : {"match" : {"description" : q_dict["1009"]}}}
    #result = es.search("products", "product", qu)
    #print(result)

    q_results = {}
    for query_id in q_dict:
        q_results[query_id] = []
        qu = {"from" : 0, "size" : 100,"query" : {"match" : {"description" : q_dict[query_id]}}}
        result = es.search("products", "product", qu)       
        top_hits_100 = result["hits"]["hits"][:100]
        #print(query_id + " " + str(len(top_hits_100)))
        i = 0
        for hit in top_hits_100:
            i += 1
            doc_id = hit["_id"]
            doc_rank = top_hits_100.index(hit) + 1
            doc_sim = hit["_score"]
            hit_list = [doc_id, doc_rank, doc_sim]
            q_results[query_id].append(hit_list)
        
        
#         if i < 100:
#                 hit_list = [["-", "-", "-"]] * (100 - i)
#                 q_results[query_id].extend(hit_list)
#         
#         if len(top_hits_100) == 0:
#                 #print("yes")
#                 hit_list = [["-", "-", "-"]] * 100
#                 q_results[query_id].extend(hit_list)
            
    #print([1, 2] * 3)
    #print(q_results["1009"])
    format_results(q_results)
    print("finished run...")     
            
def format_results(search_results):
    out_file = open(r"C:\Users\TessaElfrink\Documents\Uni\IR\dataset\run.txt", 'w', encoding="utf-8")
    out_file.write("Qid\tIter\tDocid\tRank\tSim\tRunid\n")
    
    for query_id in search_results:
        # every item in search results consists of a query id as a key with a value of a list of hits, 
        # each hit is this list is itself a list and has a structure of[doc_id, doc_rank, doc_sim]
        hits = search_results[query_id]
        for hit in hits:
            doc_id = hit[0]
            doc_rank = hit[1]
            doc_sim = hit[2]
            line = "%s\tQ0\t%s\t%s\t%s\twhitespace_alpha\n" % (query_id, doc_id, doc_rank, doc_sim)
            out_file.write(line)
    
def evaluate():
    out_file = open(r"C:\Users\TessaElfrink\Documents\Uni\IR\dataset\evaluation.txt", 'w', encoding="utf-8")
    ground_truth = open(r"C:\Users\TessaElfrink\Documents\Uni\IR\qrels.txt", encoding="utf-8")
    test_run = open(r"C:\Users\TessaElfrink\Documents\Uni\IR\dataset\run.txt", encoding="utf-8")
    
    result_dict = {}
    results = test_run.readlines()
    r1 = re.compile(r"([0-9]+)\t.+\t(.+)\t(.+)\t(.+)\t.+")
    
    # fill result_dict with pairs of a query ID and corresponding lists of doc ID, doc rank and doc sim score
    for l in results:
        line = l.rstrip()
        m = re.match(r1, line)
        if m:
            query = m.group(1)
            query = int(query)
            doc_id = m.group(2)
            rank = m.group(3)
            rank = int(rank)
            sim = m.group(4)            
            if query in result_dict:
                result_dict[query].append([doc_id, rank, sim])
            else:
                result_dict[query] = [[doc_id, rank, sim]]                      
    
    final_sums = {"P1": 0, "P5": 0, "P30": 0, "P1000": 0, "S1": 0, "S5": 0, "S30": 0, "S1000": 0,"recip_rank": 0, "map": 0}
    
    ground_truths = ground_truth.readlines() 
    #print(ground_truths)   
    cut_offs = [1, 5, 30, 1000]
    # for each query match...
    for q in result_dict:  
        #initiate for average precision
        r2 = re.compile(r"%s\sQ0\s(.)+\s1" % q)
        sum_for_map = 0
        lines_for_map = []
    
        for line in ground_truths:
            line = line.rstrip()
            m2 = re.match(r2, line)
            if m2:
                #print("match")
                sum_for_map += 1  
                lines_for_map.append(line)  
        #print(sum_for_map)
                        
        #initiate for reciprocal rank
        highest_rank = 0
        
        # for each separate cutoff...
        for cut_off in cut_offs:
            doc_list = []
            
            #sum_docs = 0
            sum_relevance = 0
            for item in result_dict[q]: 
                # check if doc rank is within cutoff  
                if item[1] <= cut_off:
                # if so, add doc_id to a list
                    doc = item[:]
                    doc_list.append(doc)
            
            
            # for all items in that list, check groundtruth file for relevance...
            for doc in doc_list:
                r3 = re.compile(r"%s\sQ0\s%s\s([0|1])" % (q, doc[0]))
                check = 0
                for line in ground_truths:
                    line = line.rstrip()
                    m = re.match(r3, line)
                    if m:
                        check = 1
                        relevance = float(m.group(1))
                        doc.append(relevance)
                        #print("match", line, doc)
                        #print(q, str(cut_off), doc)
                        # if relevant, add 1 to relevance sum
                        sum_relevance += relevance
                        # if relevant...
                        if relevance:
                            new_rank = int(doc[1])
                            if new_rank < highest_rank or highest_rank == 0:
                                highest_rank = new_rank
                                #print(q, highest_rank)
                if check == 0:
                    doc.append(0)
            
            # calculate metrics using collected data
            precision = rank_precision(sum_relevance, cut_off)
            success = rank_succes(sum_relevance)          
            #print(q, cut_off, precision, str(success))
            
            # output metric q_id score
            out_line_p = "P%d\t%d\t%.4f\n" % (cut_off, q, precision)
            out_line_s = "S%d\t%d\t%d\n" % (cut_off, q, success)
            out_file.write(out_line_p)
            out_file.write(out_line_s)
            
            final_sums["P" + str(cut_off)] += precision
            final_sums["S" + str(cut_off)] += success
        
        #print("Total", q, highest_rank)
        recip = recip_rank(highest_rank)
        #print(recip, q, highest_rank)
        out_line_rec = "recip_rank\t%d\t%.4f\n" % (q, recip)        
        avg_prec = avg_precision(sum_for_map, lines_for_map, q, result_dict[q])
        #print("recip: ", recip,  highest_rank)
        #print("map: ", avg_prec, q, result_dict[q])
        out_line_map = "map\t%d\t%.4f\n" % (q, avg_prec)       
        out_file.write(out_line_rec)
        out_file.write(out_line_map)
        
        final_sums["recip_rank"] += recip
        final_sums["map"] += avg_prec
    
    #print(result_dict)  
    compute_overall_averages(final_sums, out_file, len(result_dict))   
    print("finished evaluation...")

def rank_precision(sum_relevance, cut_off):
    return sum_relevance / cut_off

def rank_succes(sum_relevance):
    return 1 if sum_relevance > 0 else 0

def avg_precision(s, lines, q, results):
    # (sum over ranked docs for ( precision at rank=cutoff * relevance item at rank k )) / num relev docs    
    # each doc consist of [ID, rank, similarity]
    if s == 0:
        #print(q, s)
        return 0
    
    total_sum = 0.0
    relevance_sum = 0
    for item in results:
        doc = item[:]     
        r4 = re.compile(r"%s\sQ0\s%s\s1" % (q, doc[0]))      
        cut_off = 0                
        for line in lines:
            m = re.match(r4, line)
            if m:        
                cut_off = doc[1]
                relevance_sum += 1          
                precision = rank_precision(relevance_sum, cut_off)
                total_sum += precision
                #print(relevance_sum, cut_off, precision)
    return total_sum / float(s)
            
def recip_rank(highest_rank):
    return 1.0 / float(highest_rank) if highest_rank != 0  else 0

def compute_overall_averages(final_sums, out_file, num_of_queries):
    for s in final_sums:
        average = final_sums[s] / float(num_of_queries)
        out_line = "%s\tall\t%.4f\n" % (s, average) 
        out_file.write(out_line)

delete_index()
create_index()
do_indexing()
run_queries()
evaluate()

#print(IndicesClient(es).analyze(body="1\tVDI-gereedschaphouder E1 50x32mm", analyzer="dutch"))

#print(es.get(index="products", doc_type="product", id=1, _source="body"))
#print(es.count(index="products"))

