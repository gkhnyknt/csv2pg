import ttkbootstrap as ttk  # DEĞİŞİKLİK: tkinter yerine ttkbootstrap'ı ttk olarak import ettik.
from ttkbootstrap.constants import * # DEĞİŞİKLİK: Tkinter sabitleri için (LEFT, RIGHT, BOTH vs.)
from tkinter import filedialog, messagebox, scrolledtext
import pandas as pd
import psycopg2
import os
import glob
import re
import threading

# Kolon ve tablo adlarını temizleme/güvenli hale getirme fonksiyonu (Değişiklik yok)
def sanitize_db_identifier(name):
    """
    PostgreSQL tablo/sütun adları için bir ismi güvenli hale getirir.
    Boşlukları ve özel karakterleri alt çizgi ile değiştirir, küçük harfe çevirir.
    """
    name = str(name).strip().lower()
    name = re.sub(r'[\s\-.\(\)]+', '_', name)
    name = re.sub(r'[^\w_]', '', name)
    name = re.sub(r'__+', '_', name)
    if not name:
        name = "unnamed_identifier"
    if name[0].isdigit():
        name = '_' + name
    name = name.strip('_')
    return name[:63]

# --- Ana İşlem Fonksiyonu (GUI'den Bağımsız - Değişiklik yok) ---
def process_csv_to_postgres(db_config, schema_name, folder_path, status_callback):
    """
    Belirtilen klasördeki CSV dosyalarını okur ve PostgreSQL veritabanına aktarır.
    """
    try:
        status_callback(f"PostgreSQL'e bağlanılıyor: {db_config.get('host')}:{db_config.get('port')}/{db_config.get('dbname')}")
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        status_callback("Bağlantı başarılı.")

        safe_schema_name = sanitize_db_identifier(schema_name)
        if not safe_schema_name:
            status_callback("HATA: Geçersiz şema adı. İşlem durduruldu.")
            return

        status_callback(f"Şema '{safe_schema_name}' kontrol ediliyor/oluşturuluyor...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS \"{safe_schema_name}\";")
        conn.commit()
        status_callback(f"Şema '{safe_schema_name}' hazır.")

        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
        if not csv_files:
            status_callback("Bilgi: Seçilen klasörde CSV dosyası bulunamadı.")
            cur.close()
            conn.close()
            return

        total_files = len(csv_files)
        processed_count = 0

        for csv_path in csv_files:
            base_name = os.path.splitext(os.path.basename(csv_path))[0]
            table_name = sanitize_db_identifier(base_name)
            qualified_table_name = f'"{safe_schema_name}"."{table_name}"'

            status_callback(f"İşleniyor ({processed_count+1}/{total_files}): {base_name}.csv -> Tablo: {table_name}")

            try:
                df = pd.read_csv(csv_path)
            except Exception as e:
                status_callback(f"  HATA: {base_name}.csv okunamadı: {e}")
                continue

            if df.empty:
                status_callback(f"  UYARI: {base_name}.csv dosyası boş, atlanıyor.")
                continue

            original_columns = df.columns.tolist()
            safe_columns = []
            seen_cols = set()
            for i, col in enumerate(original_columns):
                s_col = sanitize_db_identifier(col if pd.notna(col) else f"col_{i}")
                if not s_col: s_col = f"unnamed_col_{i}"
                temp_col_name = s_col
                counter = 1
                while temp_col_name in seen_cols:
                    temp_col_name = f"{s_col}_{counter}"
                    counter += 1
                s_col = temp_col_name
                safe_columns.append(s_col)
                seen_cols.add(s_col)

            df.columns = safe_columns
            quoted_safe_columns = [f'"{col}"' for col in safe_columns]

            try:
                status_callback(f"  '{qualified_table_name}' tablosu siliniyor (varsa)...")
                cur.execute(f'DROP TABLE IF EXISTS {qualified_table_name};')

                status_callback(f"  '{qualified_table_name}' tablosu oluşturuluyor...")
                create_sql = f'CREATE TABLE {qualified_table_name} (\n'
                create_sql += ",\n".join([f'{col_name} TEXT' for col_name in quoted_safe_columns])
                create_sql += "\n);"
                cur.execute(create_sql)

                temp_csv_for_copy = os.path.join(folder_path, f"temp_copy_{table_name}.csv")
                df.to_csv(temp_csv_for_copy, index=False, header=False, encoding='utf-8', quoting=1)

                status_callback(f"  '{qualified_table_name}' tablosuna veri kopyalanıyor...")
                with open(temp_csv_for_copy, 'r', encoding='utf-8') as f:
                    copy_sql = f"COPY {qualified_table_name} ({', '.join(quoted_safe_columns)}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, ENCODING 'UTF8')"
                    cur.copy_expert(sql=copy_sql, file=f)
                
                conn.commit()
                status_callback(f"  '{qualified_table_name}' tablosu başarıyla yüklendi.")
                os.remove(temp_csv_for_copy)
                processed_count += 1

            except psycopg2.Error as e:
                conn.rollback()
                status_callback(f"  PostgreSQL HATA ({qualified_table_name}): {e}")
            except Exception as e:
                conn.rollback()
                status_callback(f"  Genel HATA ({qualified_table_name}): {e}")

        status_callback(f"Toplam {processed_count}/{total_files} CSV dosyası başarıyla işlendi/denendi.")

    except psycopg2.OperationalError as e:
        status_callback(f"PostgreSQL Bağlantı HATASI: {e}")
    except Exception as e:
        status_callback(f"Beklenmedik bir HATA oluştu: {e}")
    finally:
        if 'conn' in locals() and conn and not conn.closed:
            cur.close()
            conn.close()
            status_callback("PostgreSQL bağlantısı kapatıldı.")

# --- Modern GUI Sınıfı ---
class CsvToPostgresApp:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("CSV'den PostgreSQL'e Veri Aktarımı")
        self.root.geometry("750x700")

        # --- Değişkenler ---
        self.host_var = ttk.StringVar(value='localhost')
        self.port_var = ttk.StringVar(value='5432')
        self.dbname_var = ttk.StringVar(value='postgres')
        self.user_var = ttk.StringVar(value='postgres')
        self.password_var = ttk.StringVar(value='postgres')
        self.schema_var = ttk.StringVar(value='public')
        self.folder_path_var = ttk.StringVar(value="Lütfen CSV dosyalarının bulunduğu klasörü seçin.")
        self.selected_folder_internal = ""

        # --- Çerçeveler ---
        # DEĞİŞİKLİK: Tüm widget'lar ttk versiyonları ile değiştirildi.
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=BOTH, expand=YES)

        db_frame = ttk.LabelFrame(main_frame, text="PostgreSQL Bağlantı Bilgileri", padding="10", bootstyle="info")
        db_frame.pack(fill=X, pady=10)

        folder_frame = ttk.LabelFrame(main_frame, text="CSV Dosya Kaynağı", padding="10", bootstyle="info")
        folder_frame.pack(fill=X, pady=10)

        action_frame = ttk.Frame(main_frame, padding="10")
        action_frame.pack(fill=X)

        log_frame = ttk.LabelFrame(main_frame, text="İşlem Günlüğü", padding="10", bootstyle="info")
        log_frame.pack(fill=BOTH, expand=YES, pady=10)

        # --- PostgreSQL Bağlantı Bilgileri ---
        labels = ["Host:", "Port:", "Veritabanı Adı:", "Kullanıcı Adı:", "Şifre:", "Şema Adı:"]
        variables = [self.host_var, self.port_var, self.dbname_var, self.user_var, self.password_var, self.schema_var]
        
        for i, label_text in enumerate(labels):
            ttk.Label(db_frame, text=label_text).grid(row=i, column=0, sticky="w", padx=5, pady=5)
            entry_widget = ttk.Entry(db_frame, textvariable=variables[i], width=50)
            if label_text == "Şifre:":
                entry_widget.config(show="*")
            entry_widget.grid(row=i, column=1, sticky="ew", padx=5, pady=5)
        db_frame.grid_columnconfigure(1, weight=1)

        # --- CSV Klasörü Seçimi ---
        self.folder_display_label = ttk.Label(folder_frame, textvariable=self.folder_path_var, wraplength=600, justify=LEFT)
        self.folder_display_label.pack(side=LEFT, fill=X, expand=YES, padx=5)
        
        self.select_folder_button = ttk.Button(folder_frame, text="Klasör Seç", command=self.select_folder, width=15, bootstyle="info")
        self.select_folder_button.pack(side=RIGHT, padx=5)

        # --- Aktarım Butonu ---
        # DEĞİŞİKLİK: Buton stili bootstyle ile belirlendi. bg, font, height kaldırıldı, tema hallediyor.
        self.transfer_button = ttk.Button(action_frame, text="Veritabanına Aktar", command=self.start_transfer_thread, bootstyle="success")
        self.transfer_button.pack(fill=X, ipady=10) # ipady ile butonu daha dolgun yaptık.

        # --- Durum/Log Mesajları ---
        # DEĞİŞİKLİK: ScrolledText'in arkaplan ve yazı renkleri temaya uygun hale getirildi.
        self.status_text = scrolledtext.ScrolledText(log_frame, height=15, width=80, wrap=WORD, relief="sunken", borderwidth=1, state='disabled', font=('Consolas', 10),
                                                    bg="#1e1e1e", fg="#d4d4d4", insertbackground="#ffffff")
        self.status_text.pack(fill=BOTH, expand=YES)

    def select_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.selected_folder_internal = folder_selected
            self.folder_path_var.set(f"Seçilen Klasör: {self.selected_folder_internal}")
            self.log_status(f"Klasör seçildi: {self.selected_folder_internal}")
        else:
            self.folder_path_var.set("Lütfen CSV dosyalarının bulunduğu klasörü seçin.")
            self.log_status("Klasör seçme işlemi iptal edildi veya klasör seçilmedi.")

    def log_status(self, message):
        self.status_text.configure(state='normal')
        self.status_text.insert(END, message + "\n")
        self.status_text.see(END)
        self.status_text.configure(state='disabled')
        self.root.update_idletasks()

    def start_transfer_thread(self):
        db_config = {
            'host': self.host_var.get(),
            'port': self.port_var.get(),
            'dbname': self.dbname_var.get(),
            'user': self.user_var.get(),
            'password': self.password_var.get()
        }
        try:
            db_config['port'] = int(self.port_var.get())
        except ValueError:
            messagebox.showerror("Hata", "Port numarası geçerli bir sayı olmalıdır.")
            return

        schema_name = self.schema_var.get().strip()
        folder_path = self.selected_folder_internal

        if not all([db_config['host'], db_config['port'], db_config['dbname'], db_config['user'], schema_name]):
            messagebox.showerror("Eksik Bilgi", "Lütfen tüm PostgreSQL bağlantı bilgilerini ve şema adını girin.")
            return
        if not db_config['password']:
             if not messagebox.askyesno("Şifre Eksik", "PostgreSQL şifresi girmediniz. Devam etmek istiyor musunuz?"):
                return

        if not folder_path or not os.path.isdir(folder_path):
            messagebox.showerror("Eksik Bilgi", "Lütfen geçerli bir CSV dosyalarının bulunduğu klasörü seçin.")
            return

        self.transfer_button.config(state=DISABLED, text="Aktarılıyor...")
        self.status_text.configure(state='normal')
        self.status_text.delete('1.0', END)
        self.status_text.configure(state='disabled')
        self.log_status("Aktarım işlemi başlatılıyor...")

        transfer_thread = threading.Thread(
            target=process_csv_to_postgres, 
            args=(db_config, schema_name, folder_path, self.log_status_thread_safe),
            daemon=True
        )
        transfer_thread.start()
        self.root.after(100, self.check_thread_status, transfer_thread)

    def check_thread_status(self, thread):
        if thread.is_alive():
            self.root.after(100, self.check_thread_status, thread)
        else:
            self.enable_transfer_button()

    def log_status_thread_safe(self, message):
        self.root.after(0, self.log_status, message)

    def enable_transfer_button(self):
        self.transfer_button.config(state=NORMAL, text="Veritabanına Aktar")

if __name__ == "__main__":
    # DEĞİŞİKLİK: Ana pencereyi ttk.Window ile oluşturup tema adını veriyoruz.
    main_root = ttk.Window(themename="darkly") 
    app = CsvToPostgresApp(main_root)
    main_root.mainloop()
