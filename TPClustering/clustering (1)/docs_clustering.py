'''
INF-230 Tutorial on Clustering
Mario Sozio, Oana Balalau, Luis Galarraga

Simple program to test K-Means and Agglomerative Clustering on
a documents corpus.

'''
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import AgglomerativeClustering
from optparse import OptionParser
from sklearn.cluster import KMeans
from corpus import WikiCorpus
from scipy import sparse
from scipy.spatial import distance
from collections import namedtuple
from time import time
from math import log
import sys
import operator

## Topics
topics = ['music', 'sports', 'science', 'media', 'politics']

## Supported clustering methods
supported_algorithms = ['k-means', 'k-means++', 'agglomerative']
supported_labels = ['topic', 'type']
linkage_criteria = ['average', 'complete', 'ward']

'''
It generates a document-term matrix for a given corpus (an object of class WikiCorpus). 
The entry in position [i, j] contains the tf-idf score of the j-th term in the i-th document.
This method returns a sparse.crc_matrix (sparse matrix), the ground truth clustering as 
an array of pairs (doc-title, doc-topic) and the vectorizer object.
'''
def vectorize(corpus) :
    ## This object does all the job. For more information about
    ## the semantics of the arguments, please read the documentation at
    ## http://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html
    ## TfidfVectorizer takes care of the stop-words.
    vectorizer = TfidfVectorizer(max_df = 0.9,
                                 min_df = 50, stop_words='english',
                                 use_idf = True)    
    ## We store the titles_and_categories and the text of the articles in different lists.
    texts = []
    titles_and_categories = []
    ## Iterating the corpus
    for doc in corpus :
        texts.append(doc.text)
        ## We store the title and the category as a pair
        ## The category is the topic of the article. We will use
        ## it as ground truth when calculating purity and entropy
        titles_and_categories.append((doc.title, doc.category))
        
    ## This call constructs the document-term matrix. It returns a sparse matrix of
    ## type http://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.sparse.csr_matrix.html
    doc_term_matrix = vectorizer.fit_transform(texts);
    ## We return the ground truth clustering, the document-term matrix and the vectorizer object
    return titles_and_categories, doc_term_matrix, vectorizer


def agglomerative(doc_term_matrix, k, linkage) :
    ## Documentation here:
    ## http://scikit-learn.org/stable/modules/generated/sklearn.cluster.AgglomerativeClustering.html
    agg = AgglomerativeClustering(n_clusters=k, linkage=linkage)
    print("Clustering sparse data with %s" % agg)
    t0 = time()
    ## This call does the job but it requires a dense doc_term_matrix.
    agg.fit(doc_term_matrix.todense())
    print("done in %0.3fs" % (time() - t0))
    return agg;

def kmeans(doc_term_matrix, k, centroids='random', max_iterations=300) :
    ## Documentation here:
    ## http://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html#sklearn.cluster.KMeans
    km = KMeans(n_clusters=k, init=centroids, max_iter=max_iterations, n_init=10)
    print("Clustering sparse data with %s" % km)
    t0 = time()
    km.fit(doc_term_matrix)
    print("done in %0.3fs" % (time() - t0))
    return km

'''
It computes the sum of square error of a clustering obtained with K-Means or K-Means++.
'''
def sse(clustering, doc_term_matrix) :
    ## clustering.cluster_centers_ is a numpy.array of size k (the number of clusters)
    ## containing the centroids of each cluster. The element in position i is the 
    ## centroid of the i-th cluster. 
    ## See https://scipy-lectures.github.io/intro/numpy/array_object.html
    centroids = clustering.cluster_centers_
    totalSSE = 0.0
    ## clustering._labels_ is an array of size N = number of documents
    ## that stores the clusters labels of the documents assigned by the clustering 
    ## algorithm. The labels are numbers between 0 and k. If clustering.labels_[i] stores 
    ## the value j (0 <= j < k), it means the i-th document was assigned to the j-th
    ## cluster. 
    for i in range(len(clustering.labels_)) :
        ## Obtain the i-th row of the matrix, that is, the vector
        ## of the i-th document        
        rowi = doc_term_matrix.getrow(i).toarray()
        centroidi = centroids[clustering.labels_[i]]
        ## Calculate the distance between the document vector and the centroid
        ## of the cluster it was assigned to.        
        dist = distance.euclidean(rowi, centroidi)
        totalSSE += pow(dist, 2)
    return totalSSE

'''
Builds a dictionary of the form {cluster_index: [doc_idx1, doc_idx2, ... ]}
'''
def clusterIdx2DocIdx(clustering) :
    labels2docs = {}
    n_documents = len(clustering.labels_)
    for i in range(n_documents) :
        ## Get the cluster of the i-th document
        labeli = clustering.labels_[i]        
        if labeli not in labels2docs :
            labels2docs[labeli] = [] ##Initialize the list
                        
        labels2docs[labeli].append(i)
        
    return labels2docs

'''
It computes the purity of a clustering with respect to a ground truth
clustering. Refer to 
http://nlp.stanford.edu/IR-book/html/htmledition/evaluation-of-clustering-1.html
to calculate the purity of a cluster given a ground truth. 
The ground truth is given as an array of tuples (article-title, article-topic) 
'''
def purity(clustering, ground_truth) :
    ## In this phase we are building a dictionary (hash table) of the form 
    ## {cluster_label -> [document_indexes]} for the resulting clustering. 
    ## For instance if we have 4 documents and 2 clusters
    ## this dictionary would look like:
    ## 0 : [0, 2]
    ## 1 : [1, 3]
    ## assuming docs 0, 2 are in the first cluster and docs 1, 3 are in the second.
    ##  
    labels2docs = clusterIdx2DocIdx(clustering)
    n_documents = len(clustering.labels_)
        
    ## We are ready to implement the sum of the purity
    totalSum = 0.0
    for cluster_index in labels2docs :
        ## TODO: You need to find the frequency of the most common ground truth
        ## label in the documents of the cluster. 
        ## We recommend you to build a histogram, e.g., 
        ## {sports : 3, music : 5, politics : 4, ...}
        ## Then you have to pick up the biggest count.
        histogram = {}
        doc_indexes = labels2docs[cluster_index]
        ## You can check the real label of a document in this way: ground_truth[index][1]        
        for index in doc_indexes :
            real_label = ground_truth[index][1]
            if real_label not in histogram :
                histogram[real_label] = 0
            histogram[real_label] += 1    
            
        (maxkey, maxvalue) = max(histogram.iteritems(), key=operator.itemgetter(1))   
        totalSum += maxvalue
           
    return totalSum / n_documents

'''
It computes the entropy of a clustering with respect to a ground truth
clustering. 
The ground truth is given as an array of tuples (article-title, article-topic) 
'''
def entropy(clustering, ground_truth) :
    ## In this phase we are building a dictionary (hash table) of the form 
    ## {cluster_label -> [document_indexes]} for the resulting clustering. 
    ## For instance if we have 4 documents and 2 clusters
    ## this dictionary would look like:
    ## 0 : [0, 2]
    ## 1 : [1, 3]
    ## where docs 0, 2 are in the first cluster and docs 1, 3 are in the second.
    ##  
    labels2docs = clusterIdx2DocIdx(clustering)
    n_documents = len(clustering.labels_)
            
    ## For each cluster we have the indexes of the documents
    ## that belong to that cluster. To compute the purity we need
    ## to count the most common cluster_index that appears from the gold
    ## standard
    totalSum = 0.0
    for cluster_index in labels2docs :
        ## TODO: You need to find the frequencies of ALL ground truth labels in the
        ## cluster to calculate the term p_{wc} * log(p_{wc}) in the formula (look at the
        ## exercise description)
        histogram = {}
        doc_indexes = labels2docs[cluster_index]
        ## You can check the real label of a document in this way: ground_truth[index][1]        
        for index in doc_indexes :
            real_label = ground_truth[index][1]
            if real_label not in histogram :
                histogram[real_label] = 0
            histogram[real_label] += 1    
        
        innerSum = 0.0    
        for frequency in histogram.values() :
            pwc = float(frequency) / len(doc_indexes)
            innerSum += float(pwc) * log(pwc, 2)                    
        totalSum += innerSum
           
    return -totalSum / n_documents

def output (clustering, titles_and_labels, output) :
    labels2docs = clusterIdx2DocIdx(clustering)
    with open(output, 'wb') as fout :
        for cluster_index in labels2docs :
            fout.write('Cluster ' + str(cluster_index) + '\n')
            doc_indexes = labels2docs[cluster_index]
            doc_titles = []
            for index in doc_indexes :
                doc_titles.append(titles_and_labels[index][0])
            fout.write(', '.join(doc_titles) + '\n') 
        

def createTestClustering() :
    TestClustering = namedtuple('TestClustering', ['labels_'])
    testTuple = TestClustering(labels_ = [0, 1, 0, 1, 0, 1, 0, 1])
    groundTruth = [('doc1', 'sports'), ('doc2', 'music'), 
                   ('doc3', 'music'), ('doc4', 'sports'),
                   ('doc5', 'sports'), ('doc6', 'music'),
                   ('doc7', 'music'), ('doc8', 'sports')]
    return testTuple, groundTruth

def testPurity() :
    ## In this routine, we will create a fake clustering
    testClustering, groundTruth = createTestClustering()
    purityValue = purity(testClustering, groundTruth)
    return purityValue == 0.5

def testEntropy() :
    testClustering, groundTruth = createTestClustering()
    entropyValue = entropy(testClustering, groundTruth)
    return entropyValue == 0.25
    
## Main program    
if __name__ == '__main__' :
    # parse commandline arguments
    op = OptionParser()    
    op.add_option("--algorithm", action="store",
                  dest="algorithm",
                  default="k-means",
                  help="Define the algorithm used for clustering: k-means, agglomerative, k-means++")
    op.add_option("--output", action="store", 
                  dest="output", default="clusters",
                  help='''The output file where the clusters of documents will be written. The program
                  outputs only the titles of the documents. If not specified, the output is written
                  in a text file named "clusters" located in the current directory of the command line.''')
    op.add_option("--linkage", action="store", 
                  dest="linkage", default="ward",
                  help='''Applicable only for agglomerative clustering: ward, complete, average. 
                  It determines the linkage criterion used to compute the distance between two clusters.''')    
    op.add_option("--k", action="store", type=int,
                  dest="k", default=5,
                  help='''Number of clusters to find. Default 5''')
    
    print(__doc__)
    (opts, args) = op.parse_args()
    if len(args) == 0:
        op.error("No input corpus provided!")
        sys.exit(1)
    
    if opts.algorithm not in supported_algorithms :
        op.error("Unsupported method " + opts.algorithm)
        op.print_help()
        sys.exit(1)
        
    if opts.linkage not in linkage_criteria :
        op.error("Unrecognized linkage criteria: " + opts.linkage)
        op.print_help()
        sys.exit(1)
        
    ## Convert the corpus into a matrix representation
    print "Loading corpus file: " + args[0]
    titles_and_labels, doc_term_matrix, vectorizer = vectorize(WikiCorpus(open(args[0]), True))
    ## Perform the clustering
    if opts.algorithm == 'agglomerative' :
        clustering = agglomerative(doc_term_matrix, opts.k, opts.linkage)
    else :
        centroidsSelect = 'random' if opts.algorithm == 'k-means' else 'k-means++'
        clustering = kmeans(doc_term_matrix, k = opts.k, centroids = centroidsSelect)
        
        ## Printing information about the centroids
        print("Top terms per cluster:")
        order_centroids = clustering.cluster_centers_.argsort()[:, ::-1]
        terms = vectorizer.get_feature_names()
        for i in range(opts.k):
            print("Cluster %d:" % i)
            for ind in order_centroids[i, :10]:
                print(' %s' % terms[ind])
        
        # Calculate the sum of square error
        sseScore = sse(clustering, doc_term_matrix)
        print "Sum of square error: %.3f" %sseScore
    
    if testPurity() :
        print "Your implementation of the purity metric seems correct."
        print "Calculating purity against categories set " + str(topics)
        purity = purity(clustering, titles_and_labels)
        print "Purity of the clustering: " + str(purity)        
    else :
        print "The implementation of the purity metric seems to have a problem"    
    
    if testEntropy() :
        print "Your implementation of the entropy metric seems correct."        
        entropy = entropy(clustering, titles_and_labels)
        print "Entropy of the clustering: " + str(entropy)
    else :
        print "The implementation of the entropy metric seems to have a problem"
        
    output(clustering, titles_and_labels, opts.output)