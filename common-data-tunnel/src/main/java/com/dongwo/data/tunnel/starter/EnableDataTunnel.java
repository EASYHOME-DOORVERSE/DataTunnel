package com.dongwo.data.tunnel.starter;

import com.dongwo.data.tunnel.starter.selector.CanalTunnelImportSelector;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/3/4 4:04 PM
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(CanalTunnelImportSelector.class)
public @interface EnableDataTunnel {
}
