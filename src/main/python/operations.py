import numpy as np
from PIL import Image
from kernel import kernel

def rgb2grayscale(img):
    weights = [0.299, 0.587, 0.114] # poids des couleurs pour passer en gris sinon ca passe pas (on percoit pas les couleurs pareil d'ou les coefs)
    return np.dot(img[..., :3], weights).astype(np.uint8)[...,np.newaxis] # dot = produit matriciel // la derniere etape permet de garder une 3eme dimension qui vaut 1 au lieu de passer en 2d (utile pour eviter les if)

def preprocess(img,kernel, grayscale = False, offset = 0,path = None):
    if grayscale:
        img = rgb2grayscale(img)

    if img.ndim == 2:
        img = img[...,np.newaxis]

    img = img.astype(np.float32)
    x, y, z = img.shape
    x_k, y_k = kernel.shape

    new_image = np.zeros((x - x_k + 1, y - y_k + 1,z), dtype=np.float32)

    for i in range(x_k):
        for j in range(y_k):
            new_image += img[i: i + new_image.shape[0], j: j + new_image.shape[1]] * kernel[i, j]

    new_image = np.clip(new_image + offset, 0, 255).astype(np.uint8) # offset c'est pour decaler parce que les kernels peuvent mettre des valeurs dans le négatif qui se font ecraser a 0
    new_image = new_image.astype(np.uint8)
    if path:
        Image.fromarray(np.squeeze(new_image)).save(path) # squeeze permet retirer les axes de 1 dimension (si l'image est grayscale)(Image n'accepte pas)
    return new_image