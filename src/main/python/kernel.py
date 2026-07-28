import numpy as np
# Liste de kernel utilisables
class kernel :
    base = np.array([[0,0,0],[0,1,0],[0,0,0]])
    edge_detection = np.array([[1, 0, -1], [0, 0, 0], [-1, 0, 1]])
    edge_detection_2 = np.array([[0, 1, 0], [1, -4, 1], [0, 1, 0]])
    edge_detection_3 = np.array([[-1, -1, -1], [-1, 8, -1], [-1, -1, -1]])
    sharpness = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]])
    blur = np.array([[1, 1, 1], [1, 1, 1], [1, 1, 1]]) * 1/9