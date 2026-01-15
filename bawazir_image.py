# image_scraper_gui.py
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import requests
from PIL import Image, ImageTk
from io import BytesIO
import json
import pandas as pd
from pathlib import Path
import csv

class BawazirImageScraper:
    def __init__(self, root):
        self.root = root
        self.root.title("Vendor Image Scraping - Bawazir")
        self.root.geometry("600x750")
        self.root.configure(bg="#f5f5f5")
        
        # Header
        header = tk.Frame(root, bg="#2f7676", height=100)
        header.pack(fill=tk.X)
        
        title = tk.Label(
            header, 
            text="Vendor Image Scraping", 
            font=("Arial", 24, "bold"),
            bg="#2f7676", 
            fg="white"
        )
        title.pack(pady=10)
        
        subtitle = tk.Label(
            header, 
            text="Bawazir - Images", 
            font=("Arial", 12),
            bg="#2f7676", 
            fg="#bdc3c7"
        )
        subtitle.pack()
        
        # Main content
        main_frame = tk.Frame(root, bg="white", padx=30, pady=30)
        main_frame.pack(pady=40, padx=20, fill=tk.BOTH, expand=True)
        
        # Title
        content_title = tk.Label(
            main_frame,
            text="Enter Barcode",
            font=("Arial", 18, "bold"),
            bg="white"
        )
        content_title.pack(pady=(0, 20))
        
        # Barcode input
        self.barcode_entry = tk.Entry(
            main_frame,
            font=("Arial", 14),
            width=30
        )
        self.barcode_entry.insert(0, "e.g. 3337875694469")
        self.barcode_entry.bind("<FocusIn>", self.clear_placeholder)
        self.barcode_entry.pack(pady=10, ipady=8)
        
        # Search button
        search_btn = tk.Button(
            main_frame,
            text="Search",
            font=("Arial", 14),
            bg="#3498db",
            fg="white",
            cursor="hand2",
            command=self.search_image,
            relief=tk.FLAT,
            padx=20,
            pady=10
        )
        search_btn.pack(pady=10, fill=tk.X)
        
        # Separator
        separator = ttk.Separator(main_frame, orient='horizontal')
        separator.pack(fill=tk.X, pady=20)
        
        # Batch processing section
        batch_title = tk.Label(
            main_frame,
            text="Batch Processing",
            font=("Arial", 16, "bold"),
            bg="white"
        )
        batch_title.pack(pady=(0, 10))
        
        # Upload file button
        upload_btn = tk.Button(
            main_frame,
            text="Upload File (CSV/XLSX/JSON)",
            font=("Arial", 12),
            bg="#27ae60",
            fg="white",
            cursor="hand2",
            command=self.process_batch,
            relief=tk.FLAT,
            padx=20,
            pady=10
        )
        upload_btn.pack(pady=10, fill=tk.X)
        
        # Progress bar
        self.progress = ttk.Progressbar(
            main_frame,
            orient=tk.HORIZONTAL,
            length=400,
            mode='determinate'
        )
        self.progress.pack(pady=10, fill=tk.X)
        
        # Status message
        self.status_label = tk.Label(
            main_frame,
            text="",
            font=("Arial", 10),
            bg="white",
            fg="#666"
        )
        self.status_label.pack(pady=10)
        
        # Result container
        self.result_frame = tk.Frame(main_frame, bg="white")
        self.result_frame.pack(pady=20, fill=tk.BOTH, expand=True)
        
        # Footer
        footer = tk.Frame(root, bg="#2f7676", height=50)
        footer.pack(side=tk.BOTTOM, fill=tk.X)
        
        footer_text = tk.Label(
            footer,
            text="© 2025 Inventory XLSX Data Importer. All rights reserved.",
            font=("Arial", 10),
            bg="#2f7676",
            fg="white"
        )
        footer_text.pack(pady=15)
        
    def clear_placeholder(self, event):
        if self.barcode_entry.get() == "e.g. 3337875694469":
            self.barcode_entry.delete(0, tk.END)
    
    def search_image(self):
        # Clear previous results
        for widget in self.result_frame.winfo_children():
            widget.destroy()
        
        barcode = self.barcode_entry.get().strip()
        
        if not barcode or barcode == "e.g. 3337875694469":
            messagebox.showwarning("Input Required", "Please enter a barcode.")
            return
        
        self.status_label.config(text="Searching...")
        self.root.update()
        
        result = self.fetch_product_data(barcode)
        
        if result['success']:
            self.display_image(result['image_url'], result['data'])
        else:
            self.status_label.config(text=result['message'])
    
    def fetch_product_data(self, barcode):
        """Fetch product data from API"""
        url = "https://cki8cc4dwh-2.algolianet.com/1/indexes/*/queries"
        
        headers = {
            "Content-Type": "application/json",
            "x-algolia-agent": "Algolia for JavaScript (4.24.0); Browser (lite); instantsearch.js (4.78.3); react (19.2.0-canary-3fbfb9ba-20250409); react-instantsearch (7.15.8); react-instantsearch-core (7.15.8); next.js (15.3.8); JS Helper (3.25.0)",
            "x-algolia-api-key": "e8bd4ecb2b7371e80ac1dd671f5e9656",
            "x-algolia-application-id": "CKI8CC4DWH"
        }
        
        params_string = (
            f"clickAnalytics=true&facets=%5B%22ar_brand_name%22%2C%22featured%22%2C%22in_stock%22%5D"
            f"&filters=store_id%3A1&highlightPostTag=__%2Fais-highlight__"
            f"&highlightPreTag=__ais-highlight__&hitsPerPage=30&maxValuesPerFacet=2100"
            f"&page=0&query={barcode}&userToken=anonymous-a618ac7e-4cdd-4fa7-b26e-fd3f3fe40427"
        )
        
        payload = {
            "requests": [
                {
                    "indexName": "menus_production_grouping",
                    "params": params_string
                }
            ]
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if (data.get("results") and 
                len(data["results"]) > 0 and 
                data["results"][0].get("hits") and 
                len(data["results"][0]["hits"]) > 0):
                
                hit = data["results"][0]["hits"][0]
                
                if hit.get("images") and len(hit["images"]) > 0:
                    return {
                        'success': True,
                        'image_url': hit["images"][0],
                        'all_images': hit["images"],  # All image URLs
                        'data': hit,
                        'message': 'Success'
                    }
                else:
                    return {
                        'success': False,
                        'message': 'No images available',
                        'all_images': [],
                        'data': hit
                    }
            else:
                return {
                    'success': False,
                    'message': 'Product not found',
                    'all_images': []
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'Error: {str(e)}',
                'all_images': []
            }
    
    def display_image(self, image_url, hit):
        try:
            img_response = requests.get(image_url, timeout=10)
            img_response.raise_for_status()
            
            image = Image.open(BytesIO(img_response.content))
            
            max_width = 500
            if image.width > max_width:
                ratio = max_width / image.width
                new_height = int(image.height * ratio)
                image = image.resize((max_width, new_height), Image.Resampling.LANCZOS)
            
            photo = ImageTk.PhotoImage(image)
            
            img_label = tk.Label(self.result_frame, image=photo, bg="white")
            img_label.image = photo
            img_label.pack(pady=10)
            
            product_name = hit.get("en_name") or hit.get("ar_name") or "Unknown Product"
            name_label = tk.Label(
                self.result_frame,
                text=product_name,
                font=("Arial", 12, "bold"),
                bg="white"
            )
            name_label.pack(pady=5)
            
            # Show total images count
            total_images = len(hit.get("images", []))
            if total_images > 1:
                count_label = tk.Label(
                    self.result_frame,
                    text=f"({total_images} images available)",
                    font=("Arial", 9),
                    bg="white",
                    fg="#666"
                )
                count_label.pack(pady=2)
            
            url_label = tk.Label(
                self.result_frame,
                text=f"Source: {image_url}",
                font=("Arial", 8),
                bg="white",
                fg="#888"
            )
            url_label.pack(pady=5)
            
            self.status_label.config(text="Image loaded successfully.")
            
        except Exception as e:
            self.status_label.config(text=f"Failed to load image: {str(e)}")
        
    def find_barcode_column(self, df):
        """Find the barcode column in the dataframe"""
        possible_names = ['barcode', 'code', 'upc', 'ean', 'sku']
        
        import re
        
        for col in df.columns:
            # Split by common separators: underscore, dash, dot, space
            parts = re.split(r'[_\-\.\s]+', col.lower().strip())
            
            # Check if any part matches our possible names
            for part in parts:
                if part in possible_names:
                    return col
        
        return None
        
    def process_batch(self):
        """Process batch file upload"""
        file_path = filedialog.askopenfilename(
            title="Select File",
            filetypes=[
                ("All Supported", "*.csv *.xlsx *.json"),
                ("CSV files", "*.csv"),
                ("Excel files", "*.xlsx"),
                ("JSON files", "*.json")
            ]
        )
        
        if not file_path:
            return
        
        try:
            # Read file based on extension
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext == '.csv':
                df = pd.read_csv(file_path)
            elif file_ext == '.xlsx':
                df = pd.read_excel(file_path)
            elif file_ext == '.json':
                df = pd.read_json(file_path)
            else:
                messagebox.showerror("Error", "Unsupported file format")
                return
            
            # Find barcode column
            barcode_col = self.find_barcode_column(df)
            
            if not barcode_col:
                messagebox.showerror(
                    "Error", 
                    "No barcode column found. Please ensure your file has a column named 'barcode', 'code', or 'upc'"
                )
                return
            
            # Confirm processing
            total_rows = len(df)
            confirm = messagebox.askyesno(
                "Confirm Processing",
                f"Found {total_rows} barcodes in column '{barcode_col}'.\n\nDo you want to proceed?"
            )
            
            if not confirm:
                return
            
            # Process barcodes
            self.process_barcodes(df, barcode_col, file_path)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read file: {str(e)}")
    
    def process_barcodes(self, df, barcode_col, original_file):
        """Process all barcodes and save results"""
        results = []
        total = len(df)
        
        self.progress['maximum'] = total
        self.progress['value'] = 0
        
        for idx, row in df.iterrows():
            barcode = str(row[barcode_col]).strip()
            
            self.status_label.config(text=f"Processing {idx + 1}/{total}: {barcode}")
            self.root.update()
            
            # Fetch data
            result = self.fetch_product_data(barcode)
            
            # Get all image URLs
            all_images = result.get('all_images', [])
            images_str = ' | '.join(all_images) if all_images else ''
            
            # Prepare result row
            result_row = {
                'barcode': barcode,
                'status': 'Success' if result['success'] else 'Failed',
                'image_count': len(all_images),
                'image_url_1': all_images[0] if len(all_images) > 0 else '',
                'image_url_2': all_images[1] if len(all_images) > 1 else '',
                'image_url_3': all_images[2] if len(all_images) > 2 else '',
                'all_image_urls': images_str,
                'product_name_en': result.get('data', {}).get('en_name', '') if result.get('data') else '',
                'product_name_ar': result.get('data', {}).get('ar_name', '') if result.get('data') else '',
                'message': result.get('message', '')
            }
            
            results.append(result_row)
            
            # Update progress
            self.progress['value'] = idx + 1
            self.root.update()
        
        # Save results to CSV
        output_file = self.save_results(results, original_file)
        
        # Show completion message
        success_count = sum(1 for r in results if r['status'] == 'Success')
        total_images = sum(r['image_count'] for r in results)
        
        self.status_label.config(
            text=f"Completed! {success_count}/{total} products found. Results saved."
        )
        
        messagebox.showinfo(
            "Processing Complete",
            f"Processed {total} barcodes\n"
            f"Found: {success_count}\n"
            f"Not found: {total - success_count}\n"
            f"Total images: {total_images}\n\n"
            f"Results saved to:\n{output_file}"
        )
        
        self.progress['value'] = 0
    
    def save_results(self, results, original_file):
        """Save results to CSV"""
        # Generate output filename
        original_path = Path(original_file)
        output_file = original_path.parent / f"{original_path.stem}_results.csv"
        
        # Write to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if results:
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)
        
        return str(output_file)

if __name__ == "__main__":
    root = tk.Tk()
    app = BawazirImageScraper(root)
    root.mainloop()