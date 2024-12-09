import tkinter as tk
from tkinter import filedialog
import pandas as pd

def choose_file(label):
    filename = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    label.config(text=filename)
    return filename

def merge_csv():
    file1 = file1_label.cget("text")
    file2 = file2_label.cget("text")
    
    if not file1 or not file2:
        result_label.config(text="Bitte wählen Sie beide Dateien aus.")
        return
    
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    common_columns = list(set(df1.columns).intersection(df2.columns))
    
    merged_df = pd.merge(df1, df2, how="outer", on=common_columns)
    
    output_file = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
    merged_df.to_csv(output_file, index=False)
    
    result_label.config(text=f"Merge abgeschlossen. Neue Datei erstellt: {output_file}")

# GUI erstellen
root = tk.Tk()
root.title("CSV Dateien Merge")

file1_label = tk.Label(root, text="Datei 1 auswählen")
file1_label.grid(row=0, column=0)

file2_label = tk.Label(root, text="Datei 2 auswählen")
file2_label.grid(row=1, column=0)

choose_file1_button = tk.Button(root, text="Datei 1 auswählen", command=lambda: choose_file(file1_label))
choose_file1_button.grid(row=0, column=1)

choose_file2_button = tk.Button(root, text="Datei 2 auswählen", command=lambda: choose_file(file2_label))
choose_file2_button.grid(row=1, column=1)

merge_button = tk.Button(root, text="CSV Dateien mergen", command=merge_csv)
merge_button.grid(row=2, columnspan=2)

result_label = tk.Label(root, text="")
result_label.grid(row=3, columnspan=2)

root.mainloop()
