import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext # scrolledtext ekledik
import pandas as pd
import psycopg2
import os
import glob
import re
import threading # sys'i çıkardık, artık stdout yönlendirmeyeceğiz

# Kolon ve tablo adlarını temizleme/güvenli hale getirme fonksiyonu
def sanitize_db_identifier(name):
    """
    PostgreSQL tablo/sütun adları için bir ismi güvenli hale getirir.
    Boşlukları ve özel karakterleri alt çizgi ile değiştirir, küçük harfe çevirir.
    PostgreSQL'in 63 karakter sınırını da göz önünde bulundurabilir.
    """
    name = str(name).strip().lower()
    name = re.sub(r'[\s\-.\(\)]+', '_', name)  # Boşluk, tire, nokta, parantezleri _ yap
    name = re.sub(r'[^\w_]', '', name)      # Alfanümerik olmayanları (alt çizgi hariç) kaldır
    name = re.sub(r'__+', '_', name)        # Çoklu alt çizgileri tek yap
    if not name:
        name = "unnamed_identifier"
    if name[0].isdigit():
        name = '_' + name                   # Sayıyla başlıyorsa başına _ ekle
    name = name.strip('_')
    return name[:63] # PostgreSQL için genellikle 63 karakter sınırı vardır

# --- Ana İşlem Fonksiyonu (GUI'den Bağımsız) ---
def process_csv_to_postgres(db_config, schema_name, folder_path, status_callback):
    """
    Belirtilen klasördeki CSV dosyalarını okur ve PostgreSQL veritabanına aktarır.

    Args:
        db_config (dict): PostgreSQL bağlantı bilgileri.
        schema_name (str): Verilerin aktarılacağı şema adı.
        folder_path (str): CSV dosyalarının bulunduğu klasörün yolu.
        status_callback (function): Durum mesajlarını GUI'ye göndermek için callback fonksiyonu.
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
                continue # Bir sonraki dosyaya geç

            if df.empty:
                status_callback(f"  UYARI: {base_name}.csv dosyası boş, atlanıyor.")
                continue

            original_columns = df.columns.tolist()
            # Sütunları güvenli hale getir ve benzersiz yap
            safe_columns = []
            seen_cols = set()
            for i, col in enumerate(original_columns):
                s_col = sanitize_db_identifier(col if pd.notna(col) else f"col_{i}")
                if not s_col: s_col = f"unnamed_col_{i}" # Eğer sanitize sonrası boş kalırsa
                temp_col_name = s_col
                counter = 1
                while temp_col_name in seen_cols: # Benzersizliği sağla
                    temp_col_name = f"{s_col}_{counter}"
                    counter += 1
                s_col = temp_col_name
                safe_columns.append(s_col)
                seen_cols.add(s_col)

            df.columns = safe_columns # DataFrame sütunlarını güncelle
            quoted_safe_columns = [f'"{col}"' for col in safe_columns]

            try:
                status_callback(f"  '{qualified_table_name}' tablosu siliniyor (varsa)...")
                cur.execute(f'DROP TABLE IF EXISTS {qualified_table_name};')

                status_callback(f"  '{qualified_table_name}' tablosu oluşturuluyor...")
                create_sql = f'CREATE TABLE {qualified_table_name} (\n'
                create_sql += ",\n".join([f'{col_name} TEXT' for col_name in quoted_safe_columns]) # Tümünü TEXT yapıyoruz
                create_sql += "\n);"
                cur.execute(create_sql)

                temp_csv_for_copy = os.path.join(folder_path, f"temp_copy_{table_name}.csv")
                # COPY için header olmadan ve index olmadan CSV'ye yaz
                df.to_csv(temp_csv_for_copy, index=False, header=False, encoding='utf-8', quoting=1) # csv.QUOTE_ALL

                status_callback(f"  '{qualified_table_name}' tablosuna veri kopyalanıyor...")
                with open(temp_csv_for_copy, 'r', encoding='utf-8') as f:
                    copy_sql = f"COPY {qualified_table_name} ({', '.join(quoted_safe_columns)}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, ENCODING 'UTF8')"
                    cur.copy_expert(sql=copy_sql, file=f)
                
                conn.commit() # Her başarılı tablo aktarımından sonra commit
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


# --- GUI Sınıfı ---
class CsvToPostgresApp:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("CSV'den PostgreSQL'e Veri Aktarımı")
        self.root.geometry("700x650") # Pencere boyutunu biraz büyüttüm

        # --- Değişkenler ---
        self.host_var = tk.StringVar(value='localhost')
        self.port_var = tk.StringVar(value='5432')
        self.dbname_var = tk.StringVar(value='postgres')
        self.user_var = tk.StringVar(value='postgres')
        self.password_var = tk.StringVar(value='postgres')
        self.schema_var = tk.StringVar(value='public') # Genellikle 'public' varsayılandır
        self.folder_path_var = tk.StringVar(value="Lütfen CSV dosyalarının bulunduğu klasörü seçin.")
        self.selected_folder_internal = "" # Seçilen klasörün gerçek yolu için

        # --- Çerçeveler ---
        main_frame = tk.Frame(self.root, padx=10, pady=10)
        main_frame.pack(fill=tk.BOTH, expand=True)

        db_frame = tk.LabelFrame(main_frame, text="PostgreSQL Bağlantı Bilgileri", padx=10, pady=10)
        db_frame.pack(fill=tk.X, pady=5)

        folder_frame = tk.LabelFrame(main_frame, text="CSV Dosya Kaynağı", padx=10, pady=10)
        folder_frame.pack(fill=tk.X, pady=5)

        action_frame = tk.Frame(main_frame, pady=10)
        action_frame.pack(fill=tk.X)

        log_frame = tk.LabelFrame(main_frame, text="İşlem Günlüğü", padx=10, pady=10)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        # --- PostgreSQL Bağlantı Bilgileri ---
        labels = ["Host:", "Port:", "Veritabanı Adı:", "Kullanıcı Adı:", "Şifre:", "Şema Adı:"]
        variables = [self.host_var, self.port_var, self.dbname_var, self.user_var, self.password_var, self.schema_var]
        
        for i, label_text in enumerate(labels):
            tk.Label(db_frame, text=label_text).grid(row=i, column=0, sticky="w", padx=5, pady=3)
            entry_widget = tk.Entry(db_frame, textvariable=variables[i], width=50)
            if label_text == "Şifre:":
                entry_widget.config(show="*")
            entry_widget.grid(row=i, column=1, sticky="ew", padx=5, pady=3)
        db_frame.grid_columnconfigure(1, weight=1) # Giriş alanlarının genişlemesini sağla

        # --- CSV Klasörü Seçimi ---
        self.folder_display_label = tk.Label(folder_frame, textvariable=self.folder_path_var, wraplength=550, justify=tk.LEFT)
        self.folder_display_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        self.select_folder_button = tk.Button(folder_frame, text="Klasör Seç", command=self.select_folder, width=15)
        self.select_folder_button.pack(side=tk.RIGHT, padx=5)

        # --- Aktarım Butonu ---
        self.transfer_button = tk.Button(action_frame, text="Veritabanına Aktar", command=self.start_transfer_thread, bg="lightblue", font=("Arial", 12, "bold"), height=2)
        self.transfer_button.pack(fill=tk.X)


        # --- Durum/Log Mesajları ---
        self.status_text = scrolledtext.ScrolledText(log_frame, height=15, width=80, wrap=tk.WORD, relief="sunken", borderwidth=1, state='disabled', font=('Consolas', 9))
        self.status_text.pack(fill=tk.BOTH, expand=True)


    def select_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.selected_folder_internal = folder_selected
            self.folder_path_var.set(f"Seçilen Klasör: {self.selected_folder_internal}")
            self.log_status(f"Klasör seçildi: {self.selected_folder_internal}")
        else:
            # self.selected_folder_internal = "" # Zaten boş olmalı
            self.folder_path_var.set("Lütfen CSV dosyalarının bulunduğu klasörü seçin.")
            self.log_status("Klasör seçme işlemi iptal edildi veya klasör seçilmedi.")

    def log_status(self, message):
        self.status_text.configure(state='normal')
        self.status_text.insert(tk.END, message + "\n")
        self.status_text.see(tk.END)
        self.status_text.configure(state='disabled')
        self.root.update_idletasks()

    def start_transfer_thread(self):
        db_config = {
            'host': self.host_var.get(),
            'port': self.port_var.get(), # Port'u string olarak alıp, process_csv_to_postgres içinde int'e çevirebiliriz veya burada.
            'dbname': self.dbname_var.get(),
            'user': self.user_var.get(),
            'password': self.password_var.get()
        }
        try: # Port için int dönüşümü
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

        self.transfer_button.config(state=tk.DISABLED, text="Aktarılıyor...")
        self.status_text.configure(state='normal')
        self.status_text.delete('1.0', tk.END) # Önceki logları temizle
        self.status_text.configure(state='disabled')
        self.log_status("Aktarım işlemi başlatılıyor...")

        transfer_thread = threading.Thread(
            target=process_csv_to_postgres, 
            args=(db_config, schema_name, folder_path, self.log_status_thread_safe), # Thread-safe log için
            daemon=True
        )
        transfer_thread.start()
        # Thread bittiğinde butonu aktif etmek için kontrol mekanizması
        self.root.after(100, self.check_thread_status, transfer_thread)


    def check_thread_status(self, thread):
        if thread.is_alive():
            self.root.after(100, self.check_thread_status, thread)
        else:
            self.enable_transfer_button()

    def log_status_thread_safe(self, message):
        # GUI güncellemelerini ana thread üzerinden yapmak için 'after' kullan
        self.root.after(0, self.log_status, message)

    def enable_transfer_button(self):
        self.transfer_button.config(state=tk.NORMAL, text="Veritabanına Aktar")


if __name__ == "__main__":
    main_root = tk.Tk()
    app = CsvToPostgresApp(main_root)
    main_root.mainloop()