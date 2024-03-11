# Databricks notebook source
storageAcc = "epamwebinar1day"
storageAccessKey = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-03-14T02:42:09Z&st=2024-03-11T18:42:09Z&spr=https&sig=2SerDGZPnQoHZaloq%2B%2FGekCffZLUZIE7yqqtfb0AqUo%3D"

Container = "filestorage"
mountPoint = "/mnt/file1"
if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  print("mountPoint--> ",mountPoint)
  dbutils.fs.unmount(mountPoint)
  print("End of Unmount")
try:
  dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(Container, storageAcc),
    mount_point = mountPoint,
    #extra_configs = {'fs.azure.account.key.' + storageAcc + '.blob.core.windows.net': storageAccessKey}
    extra_configs = {'fs.azure.account.key.{0}.blob.core.windows.net'.format(storageAcc): storageAccessKey}
    #extra_configs = {"fs.azure.sas.{0}.blob.core.windows.net".format(Container):sasToken}
    #extra_configs = {'fs.azure.sas.' + Container + '.' + storageAcc + '.blob.core.windows.net': sasToken}
  )
  print("mount succeeded!")
except Exception as e:
  print("mount exception", e)


# COMMAND ----------

sa2 = "STORAGE_ACCOUNT_NAME"
sa2_key = "YOUR_ACCESS_KEY"

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC src_dir="mnt/test/test1"
# MAGIC dest_dir="mnt/test/test2"
# MAGIC # Get current date and time
# MAGIC current_date=$(date +%s)
# MAGIC # Loop through each file in the source directory
# MAGIC for file in "$src_dir"/*
# MAGIC do
# MAGIC     # Get the last modified date of the file
# MAGIC     file_date=$(stat -c %Y "$file")
# MAGIC     # Calculate the difference in timestamps
# MAGIC     diff=$((current_date - file_date))
# MAGIC     # If the file was modified in the last 24 hours (86400 seconds)
# MAGIC     if [ $diff -le 86400 ]
# MAGIC     then
# MAGIC         # Move the file to the destination directory
# MAGIC         mv "$file" "$dest_dir"
# MAGIC     fi
# MAGIC done

# COMMAND ----------


