#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF 診斷工具 - 檢查 PDF 檔案類型和內容
幫助判斷為什麼某些 PDF 無法處理
"""

import sys
import pdfplumber
from pathlib import Path
from pdf2image import convert_from_path
import pytesseract

def diagnose_pdf(pdf_path):
    """診斷 PDF 檔案"""
    print(f"\n{'='*60}")
    print(f"診斷檔案: {pdf_path}")
    print(f"{'='*60}")
    
    try:
        # 1. 檢查檔案資訊
        file_size = Path(pdf_path).stat().st_size / 1024  # KB
        print(f"📁 檔案大小: {file_size:.1f} KB")
        
        with pdfplumber.open(pdf_path) as pdf:
            print(f"📄 頁數: {len(pdf.pages)}")
            
            # 2. 檢查每頁的內容
            total_chars = 0
            total_images = 0
            total_tables = 0
            
            for i, page in enumerate(pdf.pages, 1):
                print(f"\n--- 第 {i} 頁 ---")
                
                # 提取文字
                text = page.extract_text()
                if text:
                    char_count = len(text)
                    total_chars += char_count
                    print(f"  ✅ 文字層: {char_count} 字元")
                    # 顯示前100個字元作為預覽
                    preview = text[:100].replace('\n', ' ')
                    print(f"  預覽: {preview}...")
                else:
                    print(f"  ❌ 無文字層")
                
                # 檢查圖片
                if hasattr(page, 'images'):
                    image_count = len(page.images)
                    total_images += image_count
                    if image_count > 0:
                        print(f"  🖼️ 圖片: {image_count} 個")
                
                # 檢查表格
                tables = page.extract_tables()
                if tables:
                    total_tables += len(tables)
                    print(f"  📊 表格: {len(tables)} 個")
                    for j, table in enumerate(tables, 1):
                        print(f"     表格{j}: {len(table)}行 x {len(table[0]) if table else 0}列")
            
            # 3. 總結
            print(f"\n{'='*40}")
            print("📋 診斷結果總結")
            print(f"{'='*40}")
            
            if total_chars > 0:
                print(f"✅ PDF 類型: 文字型 PDF")
                print(f"   總字元數: {total_chars}")
                print(f"   建議: 可直接提取文字，不需要 OCR")
            else:
                print(f"⚠️ PDF 類型: 掃描檔/圖片型 PDF")
                print(f"   總字元數: 0")
                print(f"   建議: 需要啟用 OCR 功能")
                
                # 嘗試 OCR 第一頁
                print(f"\n🔍 嘗試 OCR 處理第一頁...")
                try:
                    images = convert_from_path(pdf_path, dpi=200, first_page=1, last_page=1)
                    if images:
                        text = pytesseract.image_to_string(images[0], lang='chi_tra+eng')
                        if text:
                            print(f"✅ OCR 成功！提取到 {len(text)} 字元")
                            preview = text[:200].replace('\n', ' ')
                            print(f"OCR 預覽: {preview}...")
                        else:
                            print(f"❌ OCR 無法識別文字")
                except Exception as e:
                    print(f"❌ OCR 處理失敗: {e}")
            
            if total_images > 0:
                print(f"🖼️ 包含圖片: {total_images} 個")
            
            if total_tables > 0:
                print(f"📊 包含表格: {total_tables} 個")
                
    except Exception as e:
        print(f"❌ 診斷失敗: {e}")
        import traceback
        traceback.print_exc()

def batch_diagnose(directory):
    """批次診斷目錄中的所有 PDF"""
    pdf_files = list(Path(directory).glob("*.pdf"))
    print(f"找到 {len(pdf_files)} 個 PDF 檔案")
    
    results = {
        'text_pdf': [],
        'scan_pdf': [],
        'error': []
    }
    
    for pdf_file in pdf_files:
        try:
            with pdfplumber.open(pdf_file) as pdf:
                has_text = False
                for page in pdf.pages:
                    if page.extract_text():
                        has_text = True
                        break
                
                if has_text:
                    results['text_pdf'].append(pdf_file.name)
                else:
                    results['scan_pdf'].append(pdf_file.name)
        except Exception as e:
            results['error'].append(f"{pdf_file.name}: {e}")
    
    print(f"\n{'='*60}")
    print("批次診斷結果")
    print(f"{'='*60}")
    print(f"\n✅ 文字型 PDF ({len(results['text_pdf'])} 個):")
    for name in results['text_pdf']:
        print(f"  - {name}")
    
    print(f"\n⚠️ 掃描型 PDF ({len(results['scan_pdf'])} 個) - 需要 OCR:")
    for name in results['scan_pdf']:
        print(f"  - {name}")
    
    if results['error']:
        print(f"\n❌ 錯誤 ({len(results['error'])} 個):")
        for error in results['error']:
            print(f"  - {error}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方法:")
        print("  診斷單一檔案: python pdf_diagnostic.py <pdf_file>")
        print("  批次診斷: python pdf_diagnostic.py --batch <directory>")
        sys.exit(1)
    
    if sys.argv[1] == "--batch" and len(sys.argv) == 3:
        batch_diagnose(sys.argv[2])
    else:
        diagnose_pdf(sys.argv[1])
