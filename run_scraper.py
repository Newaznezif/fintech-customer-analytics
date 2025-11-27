from src.scraping import scrape_all

if __name__ == "__main__":
    df = scrape_all(save_to_db=False)
    print("Scraping finished! Total reviews:", len(df))
