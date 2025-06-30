ssh testonaylab@10.114.131.140
cd /home/testonaylab/projeler/sensor

SİSTEMİ DURDUR
sudo systemctl stop sensor_logger.service
DOSYA ADRESİNE GİT
cd /home/testonaylab/projeler/sensor
REPOYU ÇEK (dosyaların üzerine yazar)
git clone https://github.com/kaganulusoy/TOLabPiZero.git /home/testonaylab/projeler/sensor
DOSYAYI MANUEL DEĞİŞTİRİP KAYDET
sudo systemctl start sensor_logger.service
