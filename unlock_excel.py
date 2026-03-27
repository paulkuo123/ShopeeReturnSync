import msoffcrypto
import openpyxl
import io
import os

password = "456798"
input_folder = "./退貨單未解密"
output_folder = "./退貨單"

os.makedirs(output_folder, exist_ok=True)

for filename in os.listdir(input_folder):
    if filename.endswith(".xlsx"):
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(output_folder, "no_pw_" + filename)

        try:
            with open(input_path, "rb") as f:
                office_file = msoffcrypto.OfficeFile(f)
                office_file.load_key(password=password)

                decrypted = io.BytesIO()
                office_file.decrypt(decrypted)

                wb = openpyxl.load_workbook(decrypted)
                wb.save(output_path)

            print(f"✅ 成功: {filename}")

        except Exception as e:
            print(f"❌ 失敗: {filename} | {e}")

print("全部處理完成")