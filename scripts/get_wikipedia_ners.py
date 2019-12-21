#coding=utf-8

import xml.etree.ElementTree as ET
import re
from nltk.tokenize import word_tokenize
from nltk.tag import StanfordNERTagger
from itertools import groupby
from pyspark import SparkFiles

spaces = re.compile(r' {2,}')
dots = re.compile(r'\.{4,}')
tailRE = re.compile('\w+')

from pyspark import SparkContext
sc =SparkContext()

def parseXML(xmlfile):
    root = ET.fromstring(xmlfile[1].encode('utf8'))
    print(root)
    print(root.findall('./text'))
    for child in root.iter('revision'):
        description = child.find('text').text
        return description

def dropSpans(spans, text):
    """
    Drop from text the blocks identified in :param spans:, possibly nested.
    """
    spans.sort()
    res = ''
    offset = 0
    for s, e in spans:
        if offset <= s:         # handle nesting
            if offset < s:
                res += text[offset:s]
            offset = e
    res += text[offset:]
    return res

def clean_text(text):
    spans = []
    comment = re.compile(r'<!--.*?-->', re.DOTALL)
    for m in comment.finditer(text):
            spans.append((m.start(), m.end()))
    text = dropSpans(spans, text)
    text = text.replace('\t', ' ')
    text = spaces.sub(' ', text)
    text = dots.sub('...', text)
    text = re.sub(' (,:\.\)\]»)', r'\1', text)
    text = re.sub('(\[\(«) ', r'\1', text)
    text = re.sub(r'\n\W+?\n', '\n', text, flags=re.U)  # lines with only punctuations
    text = text.replace(',,', ',').replace(',.', '.')
    text = re.sub(r'!(?:\s)?style=\"[a-z]+:(?:\d+)%;\"', r'', text)
    text = re.sub(r'!(?:\s)?style="[a-z]+:(?:\d+)%;[a-z]+:(?:#)?(?:[0-9a-z]+)?"', r'', text)
    text = text.replace('|-', '')
    text = text.replace('|', '')
    return text

def dropNested(text, openDelim, closeDelim):
    """
    A matching function for nested expressions, e.g. namespaces and tables.
    """
    openRE = re.compile(openDelim, re.IGNORECASE)
    closeRE = re.compile(closeDelim, re.IGNORECASE)
    # partition text in separate blocks { } { }
    spans = []                  # pairs (s, e) for each partition
    nest = 0                    # nesting level
    start = openRE.search(text, 0)
    if not start:
        return text
    end = closeRE.search(text, start.end())
    next = start
    while end:
        next = openRE.search(text, next.end())
        if not next:            # termination
            while nest:         # close all pending
                nest -= 1
                end0 = closeRE.search(text, end.end())
                if end0:
                    end = end0
                else:
                    break
            spans.append((start.start(), end.end()))
            break
        while end.end() < next.start():
            # { } {
            if nest:
                nest -= 1
                # try closing more
                last = end.end()
                end = closeRE.search(text, end.end())
                if not end:     # unbalanced
                    if spans:
                        span = (spans[0][0], last)
                    else:
                        span = (start.start(), last)
                    spans = [span]
                    break
            else:
                spans.append((start.start(), end.end()))
                # advance start, find next close
                start = next
                end = closeRE.search(text, next.end())
                break           # { }
        if next != start:
            # { { }
            nest += 1
    # collect text outside partitions
    return dropSpans(spans, text)


def remove_paranthesis(text):
    text = dropNested(text, r'{{', r'}}')
    return text

def findBalanced(text, openDelim=['[['], closeDelim=[']]']):
    openPat = '|'.join([re.escape(x) for x in openDelim])
    # pattern for delimiters expected after each opening delimiter
    afterPat = {o: re.compile(openPat + '|' + c, re.DOTALL) for o, c in zip(openDelim, closeDelim)}
    stack = []
    start = 0
    cur = 0
    # end = len(text)
    startSet = False
    startPat = re.compile(openPat)
    nextPat = startPat
    while True:
        next = nextPat.search(text, cur)
        if not next:
            return
        if not startSet:
            start = next.start()
            startSet = True
        delim = next.group(0)
        if delim in openDelim:
            stack.append(delim)
            nextPat = afterPat[delim]
        else:
            opening = stack.pop()
            # assert opening == openDelim[closeDelim.index(next.group(0))]
            if stack:
                nextPat = afterPat[stack[-1]]
            else:
                yield start, next.end()
                nextPat = startPat
                start = next.end()
                startSet = False
        cur = next.end()

def replaceInternalLinks(text):
    cur = 0
    res = ''
    for s, e in findBalanced(text):
        m = tailRE.match(text, e)
        if m:
            trail = m.group(0)
            end = m.end()
        else:
            trail = ''
            end = e
        inner = text[s + 2:e - 2]
        # find first |
        pipe = inner.find('|')
        if pipe < 0:
            title = inner
            label = title
        else:
            title = inner[:pipe].rstrip()
            # find last |
            curp = pipe + 1
            for s1, e1 in findBalanced(inner):
                last = inner.rfind('|', curp, s1)
                if last >= 0:
                    pipe = last  # advance
                curp = e1
            label = inner[pipe + 1:].strip()
        res += text[cur:s] + title + trail
        cur = end
    return res + text[cur:]

def extract_ner(_list):
    tokenized_text = word_tokenize(_list)
    stanford_classifier = SparkFiles.get("english.all.3class.distsim.crf.ser.gz")
    stanford_ner_path = SparkFiles.get("stanford-ner.jar")
    st = StanfordNERTagger(stanford_classifier, stanford_ner_path, encoding='utf-8')
    classified_text = st.tag(tokenized_text)
    # ans = []
    dic = {}
    for tag, chunk in groupby(classified_text, lambda x:x[1]):
        if tag != "O":
            word = " ".join(w for w, t in chunk)
            dic[word] = dic.get(word, 0) + 1
            # ans.append((word, tag))
    # for item in dic.items():
    #     ans.append(item)
    return dic.items()

#vals = sc.parallelize(["Mihir has done something."])
#vals.saveAsTextFile("hdfs:/user/mihir/mihir_test_cluster5/")

data = sc.newAPIHadoopFile( "hdfs:/user/mihir/entWiki/-" , 'com.databricks.spark.xml.XmlInputFormat','org.apache.hadoop.io.Text','org.apache.hadoop.io.Text' ,conf = {'xmlinput.start' : '<page>', 'xmlinput.end' : '</page>'})
text_data = data.map(parseXML).map(clean_text).map(remove_paranthesis).map(replaceInternalLinks)
vals = text_data.flatMap(extract_ner)
counts = vals.reduceByKey(lambda x,y:x+y)
print("Done complete")
counts.saveAsTextFile("hdfs:/user/mihir/zcluster20200.txt")