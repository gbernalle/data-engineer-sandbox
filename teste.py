lista = [0,5,8,9,7,10]

for ele in lista[::-1]:
  print(ele)

lista.reverse()
lista.pop()


lista.sort()

print(lista)

lista.insert(2,15)

print(f'Valor da lista é {lista}')
print(lista.index(7))

val1,val2,*_ = lista
print(val1,val2, _)


# Tuplas -> Só não colocar colchetes. É uma lista imutável

lista_tuplada = 'Teste1', 'Teste2', 'Teste3'

print(lista_tuplada[::-1])

# enumerate

faz_enum = enumerate(lista_tuplada)
print("--------- ------------")

print(next(faz_enum), 'Teste')

for elem in faz_enum:
  print(elem)

print("--------- ------------")
print(faz_enum) # Só mostrando que consome o último apontador

# Se quiser que o enumerate funcione igual uma lista de chave, valor

faz_enum = list(enumerate(lista_tuplada))
print(faz_enum)


def funcao_teste(tarifa=0.0) -> None:
  valores = tarifa
  print("--------------\n\t",valores)



funcao_teste()