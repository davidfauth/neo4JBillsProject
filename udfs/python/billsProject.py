from pig_util import outputSchema

import nltk
from py2neo import neo4j
from py2neo import node, rel
 
@outputSchema("top_five:bag{t:(bigram:chararray)}")
def top_5_bigrams(tweets):
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    alice=graph_db.create(node(name="Bruce Willis"))
    tokenized_tweets = [ nltk.tokenize.WhitespaceTokenizer().tokenize(t[0]) for t in tweets ]
 
    bgm    = nltk.collocations.BigramAssocMeasures()
    finder = nltk.collocations.BigramCollocationFinder.from_documents(tokenized_tweets)
    top_5  = finder.nbest(bgm.likelihood_ratio, 5)
    
    return [ ("%s %s" % (s[0], s[1]),) for s in top_5 ]

@outputSchema("t:tuple()") 
def bagToTuple(bag): 
    flat_bag = [item for tup in bag for item in tup]
    return flat_bag


@outputSchema('vals: {(val:map[])}')
def foo(the_input):
    # This converts the indeterminate number of maps into a bag.
    out = []
    for map in the_input:
        out.append(map)
    return out

@outputSchema("values:bag{t:tuple(key, value)}")
def bag_of_tuples(map_dict):
    return map_dict.items()