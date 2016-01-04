Etape 1 :
 cloner le fichier default.tfvars en XXX.tfvars et modifier le contenu
 terraform plan -var-file XXX.tfvars
 terraform apply -var-file gupinot.tfvars

Etape 2 :
 modifier le fichier ansible/vars/external_vars.yml (mots de passe)
 