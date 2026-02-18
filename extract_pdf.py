import PyPDF2

pdf_path = r'C:\Users\yli\Desktop\Apache.pdf'
pdf_reader = PyPDF2.PdfReader(pdf_path)

print(f"Total pages: {len(pdf_reader.pages)}\n")

all_text = ""
for i, page in enumerate(pdf_reader.pages):
    text = page.extract_text()
    all_text += f"\n{'='*80}\nPAGE {i+1}\n{'='*80}\n"
    all_text += text
    if i < 5:
        print(f"--- Page {i+1} ---")
        print(text[:400])
        print()

# Save full text
with open('pdf_content.txt', 'w', encoding='utf-8') as f:
    f.write(all_text)

print("\n✓ Full PDF content saved to pdf_content.txt")
