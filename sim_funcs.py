import math
import Levenshtein
import ngram
import IPy

from datetime import datetime




try:
    from ssdeep import ssdeep
    fuzzyhash = ssdeep()
except ImportError:
    import ssdeep
    fuzzyhash = ssdeep


DATE_PATTERN = '%Y-%m-%d'

N = ngram.NGram()
Ngram_compare = N.compare



'''
Geolocatin similarity

Input node shall have

1. id
2. x axis value
3. y axis value

in strict order!

'''
def sim_geolocation(node1, node2, sigma=0.5):

    x1 = float(node1[0])
    x2 = float(node2[0])

    y1 = float(node1[1])
    y2 = float(node2[1])

    x_axis = (x1 - x2) * (x1 - x2)
    y_axis = (y1 - y2) * (y1 - y2)
    
    d = math.sqrt(x_axis + y_axis)
    sim = math.exp(-(d**2) / sigma**2)
    return sim




'''
String similarity


Input node shall have

1. id
2. subject

in strict order!

'''
def sim_levenshtein(node1, node2, sigma=0, sigma_min=7):
    """
    Returns the similarity between two strings based on the Levenshtein distance.
    The distance is mapped using the sigma coefficient.
    """
    string1 = node1
    string2 = node2
    
    
    #print 'computing levenshtein similarity'
    if string1 is None or string2 is None:
        return 0.0
    if len(string1) == 0 or len(string2) == 0:
        return 0.0
        
    max_length = max(len(string1), len(string2))
    if sigma == 0:
        sigma = max(max_length/2.5, sigma_min)
    dist = float(Levenshtein.distance(string1, string2))
    simval = math.exp(-(dist**2)/float(sigma**2))
    return simval


def sim_Ngram(node1, node2):
    """
    Returns a similarity value between two texts based on the NGram similarity.
    """
    txt1 = node1
    txt2 = node2
    if txt1 is None or txt2 is None:
        return 0.0
    # check first if one of the two is empty
    if len(txt1) == 0 or len(txt2) == 0:
        return 0.0
    # then check for equality
    if txt1 == txt2:
        return 1.0
    else:
        return Ngram_compare(txt1, txt2)



def sim_IP(node1, node2, sigma=1e-7):
    addr1 = node1
    addr2 = node2
    try:
        ip1 = IPy.IP(addr1)
    except:
        print 'warning: bad IP format: %s' %str(addr1)
        return 0.0
    try:
        ip2 = IPy.IP(addr2)
    except:
        print 'warning: bad IP format: %s' %str(addr2)
        return 0.0

    dist = float((ip1.int()-ip2.int()))/(2**32-1)
    simval = math.exp(-(dist**2)/float(sigma**2))

    return simval



def sim_ssdeep(hash1, hash2):
    """ Compares two fuzzy hashes and return the degree of similarity."""
    try:
        simval = fuzzyhash.compare(hash1, hash2)/100.0
    except:
        simval = 0

    return max(simval, 0)


# @memoise
def sim_tanimoto(node1, node2):
    """
    Returns the Tanimoto coefficient between two sets of values.
    """
    a = set(node1)
    b = set(node2)

    if a is None or b is None:
        return 0.0

    if len(a) == 0 or len(b) == 0:
        return 0.0

    if a == set(['']) or b == set(['']) or a == [''] or b == ['']:
        return 0.0

    if a == b:
        return 1.0
    else:
        c = [v for v in a if v in b]
        return float(len(c))/(len(a)+len(b)-len(c))


def sim_generic(pattern1, pattern2):
    """
    Simplistic function that verifies if two patterns are the same.
    """
    if pattern1 is None or pattern2 is None:
        return 0.0

    if not isinstance(pattern1, int) and not isinstance(pattern1, long):
        if len(pattern1) == 0 :
            return 0.0
    if not isinstance(pattern2, int) and not isinstance(pattern2, long):
        if len(pattern2) == 0 :
            return 0.0

    if pattern1 == pattern2:
        return 1.0
    else:
        return 0.0



def sim_date(node1, node2, a=0, b=3, c=7):
    """
    A simple function to compute the similarity between two dates.
    (linear on the segments [a,b] and [b,c])
    """
    if node1 == [] or node2 == []:
        return 0.0
    if node1 is None or node2 is None:
        return 0.0

    date1 = datetime.strptime(node1, DATE_PATTERN)
    date2 = datetime.strptime(node2, DATE_PATTERN)

    timediff = date1 - date2
    x = abs(timediff.days)

    if x <= a:
        return 1.0
    elif x > a and x <= b:
        sim = 1 - (0.5 * (x-a)/(b-a))
        return sim
    elif x > b and x <= c:
        sim = 0.5 - (0.5 * (x-b)/(c-b))
        return sim
    else:
        return 0.0