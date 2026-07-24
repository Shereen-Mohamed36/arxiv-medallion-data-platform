import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
import time

def fetch_large_arxiv_data(categories, target_per_category=2000):
    all_papers = []
    base_url = 'http://export.arxiv.org/api/query?'
    chunk_size = 500 
    
    for category in categories:
        print(f"Starting data ingestion for category: {category}")
        for start in range(0, target_per_category, chunk_size):
            print(f"Fetching papers from index {start} to {start + chunk_size}...")
            query_url = f"{base_url}search_query=cat:{category}&start={start}&max_results={chunk_size}"
            
            try:
                response = urllib.request.urlopen(query_url)
                xml_data = response.read()
                
                root = ET.fromstring(xml_data)
                ns = {'atom': 'http://www.w3.org/2005/Atom'}
                
                entries = root.findall('atom:entry', ns)
                if not entries:
                    print(f"No more entries found for category: {category}")
                    break
                    
                for entry in entries:
                    title = entry.find('atom:title', ns).text.strip().replace('\n', ' ')
                    summary = entry.find('atom:summary', ns).text.strip().replace('\n', ' ')
                    published = entry.find('atom:published', ns).text
                    
                    authors = [author.find('atom:name', ns).text for author in entry.findall('atom:author', ns)]
                    authors_str = ", ".join(authors)
                    
                    pdf_link = ""
                    for link in entry.findall('atom:link', ns):
                        if link.attrib.get('title') == 'pdf':
                            pdf_link = link.attrib.get('href')
                    
                    all_papers.append({
                        'Title': title,
                        'Abstract': summary,
                        'Published_Date': published,
                        'Authors': authors_str,
                        'Category': category,
                        'PDF_Link': pdf_link
                    })
                
                # Respect arXiv API rate limits
                time.sleep(3) 
                
            except Exception as e:
                print(f"Error during ingestion batch: {e}")
                time.sleep(10)
                
    df = pd.DataFrame(all_papers)
    return df

if __name__ == "__main__":
    target_categories = ["cs.AI", "cs.CV", "cs.CL", "cs.LG", "cs.SE"]
    
    large_df = fetch_large_arxiv_data(target_categories, target_per_category=2000)
    
    if not large_df.empty:
        large_df = large_df.drop_duplicates(subset=['Title'])
        output_file = "large_raw_arxiv_papers.csv"
        large_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"Ingestion successful. Data saved to: {output_file}")
        print(f"Total unique rows ingested: {large_df.shape[0]}")
    else:
        print("Data ingestion failed. DataFrame is empty.")