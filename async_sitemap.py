from requests_html import HTML, HTMLSession, AsyncHTMLSession
from functools import partial
import requests
import argparse

# construct the argument parser and parser the arguments
parser = argparse.ArgumentParser()
parser.add_argument('-u', '--url', default='https://www.gutekueche.de', type=str,
                    help='url to construct sitemap from')
parser.add_argument('-n', '--maxdepth', default=3, type=int,
                    help='depth of search')
parser.add_argument('-b', '--batchsize', default=2000, type=int,
                    help='size of batches to process')
parser.add_argument('-f', '--file', default='./sitemap.txt', type=str,
                    help='path to file to store requests')

args = vars(parser.parse_args())


batch_size = args['batchsize']
max_depth = args['maxdepth']
url = args['url']
file = args['file']


def spider(session, base, max_depth, n):
    sitemap = set()
    new = {base}

    for i in range(max_depth):
        collected = collect(session, new, n, base)
        new = collected.difference(sitemap)
        sitemap.update(new)

        if len(new) == 0:
            print('all links found!')
            print('number of links = {}'.format(len(sitemap)))
            print('Depth = {}'.format(i))
            return sitemap

    print('maximum depth of {} reached'.format(max_depth))
    print('number of links = {}'.format(len(sitemap)))
    return sitemap

def collect(session, links, n, base):
    bag = set()
    print('crawling {} new link(s)...'.format(len(links)))
    batches = [list(links)[i:i + n] for i in range(0, len(links), n)]
    for k, batch in enumerate(batches):
        print('starting batch {} of {}'.format(k+1, len(batches)))
        coros = [partial(get_links, session, link, base) for link in batch]
        new =  session.run(*coros)
        bag.update(*new)
    return bag

async def get_links(session, link, base):
    if link.startswith(base):
        try:
            r = await session.get(link)
            if r.status_code == 200:
                return r.html.absolute_links
            else:
                #print('connection failed, continuing...')
                return set()
        except (requests.exceptions.MissingSchema, requests.exceptions.InvalidSchema, requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
            print('ConnectionError')
            return set()

    else:
        return set()

def sitemap(url, max_depth, batch_size):

    #browser stuff
    args = ['--lang=en-US,en','--no-sandbox']
    adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)

    #create session
    session =  AsyncHTMLSession(browser_args=args)
    session.mount('https://', adapter)

    result = spider(session, url, max_depth, batch_size)

    #close session
    session.run(session.close)

    return result

def dump(results, file):
    with open(file, 'w') as f:
        for result in results:
            f.write(result)
            f.write('\n')

if __name__ == '__main__':
    #url = 'https://www.gutekueche.de'
    #url = 'https://www.swissmilk.ch/de/'
    #max_depth = 3
    #batch_size = 2000
    sitemap = sitemap(url, max_depth, batch_size)

    print('dumping to file...')
    dump(results=sitemap, file=file)
    print('dump completed')



