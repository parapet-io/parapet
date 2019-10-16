package io.parapet.core

trait StreamSource[F[_]] {

  def open: F[Unit]

  def in: F[InStream[F]]

  def out: F[OutStream[F]]

  def close: F[Unit]

}